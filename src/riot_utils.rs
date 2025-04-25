use anyhow::{anyhow, Result};

pub struct GameId {
    pub name: String,
    pub tag: String,
}

macro_rules! riot_api {
    ($response:expr) => {{
        use http::HeaderValue;
        use std::time::Duration;
        let result = $response;
        match result {
            Result::Err(mut e) => {
                let status_code = { e.status_code().clone() };
                return match status_code {
                    None => Result::Err(anyhow!(
                        "{}",
                        match e.take_response() {
                            Some(r) => {
                                let message = r.text();
                                let text = message.await?;
                                text.clone()
                            }
                            None => String::from("<no response>"),
                        }
                    )),
                    Some(http::status::StatusCode::FORBIDDEN) => panic!("The Riot Key is bad."),
                    Some(http::status::StatusCode::TOO_MANY_REQUESTS) => match e.take_response() {
                        Some(r) => {
                            let wait_time = Duration::from_secs(
                                r.headers()
                                    .get("retry-after")
                                    .unwrap_or(&HeaderValue::from_str("2").unwrap())
                                    .to_str()?
                                    .parse::<u64>()?,
                            ) + Duration::from_millis(
                                (chrono::Local::now().timestamp_millis() % 1000 + 1000) as u64,
                            );
                            tokio::time::sleep(wait_time).await;
                            return Result::Err(anyhow!(
                                "Rate limited - had to wait {} seconds",
                                wait_time.as_secs()
                            ));
                        }
                        None => {
                            tokio::time::sleep(Duration::from_secs(2)).await;
                            return Result::Err(anyhow!(
                                "Rate limited - didn't know what to wait, so waited 2 seconds"
                            ));
                        }
                    },
                    _s => {
                        return Result::Err(anyhow!(e));
                    }
                };
            }
            Result::Ok(s) => Result::<_, anyhow::Error>::Ok(s),
        }
    }};
}

pub(crate) use riot_api;

// Helper to parse summoner name/Riot ID string
pub fn parse_summoner_input(input: &str) -> Result<GameId> {
    let text = input.trim();
    if text.is_empty() {
        return Err(anyhow!("Summoner name/Riot ID cannot be empty."));
    }
    if text.contains('#') {
        let mut parts = text.split('#');
        let name = parts
            .next()
            .ok_or_else(|| anyhow!("Invalid Riot ID format: Missing name part."))?;
        let tag = parts
            .next()
            .ok_or_else(|| anyhow!("Invalid Riot ID format: Missing tag part."))?;
        if name.is_empty() || tag.is_empty() {
            return Err(anyhow!(
                "Invalid Riot ID format: Name or tag part is empty."
            ));
        }
        Ok(GameId {
            name: name.to_string(),
            tag: tag.to_string(),
        })
    } else {
        Err(anyhow!(
            "Invalid Riot ID format: Expected format is 'name#tag'."
        ))
    }
}
