//! Access to the local League Client API (LCU) using the client's lockfile.

use std::path::PathBuf;

use anyhow::{Context, Result, anyhow};
use base64::Engine;
use serde_json::Value;

/// Credentials parsed from `LeagueClient:<pid>:<port>:<password>:<protocol>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LcuCredentials {
    pub port: u16,
    pub password: String,
}

pub fn parse_lockfile(contents: &str) -> Result<LcuCredentials> {
    let parts: Vec<&str> = contents.trim().split(':').collect();
    if parts.len() != 5 {
        return Err(anyhow!("lockfile has {} fields, expected 5", parts.len()));
    }
    let port = parts[2].parse().context("lockfile port is not a number")?;
    if parts[3].is_empty() {
        return Err(anyhow!("lockfile password is empty"));
    }
    Ok(LcuCredentials {
        port,
        password: parts[3].to_string(),
    })
}

#[derive(Debug)]
pub enum LcuError {
    /// No lockfile / connection refused: the League client isn't running.
    NotRunning(String),
    Other(anyhow::Error),
}

impl std::fmt::Display for LcuError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LcuError::NotRunning(why) => write!(f, "league client not running ({why})"),
            LcuError::Other(e) => write!(f, "{e}"),
        }
    }
}

/// Minimal LCU surface the relay needs; mocked in tests.
pub trait LcuApi {
    /// Performs a GET against the LCU and returns (HTTP status, JSON body).
    fn get(&self, path: &str) -> impl std::future::Future<Output = Result<(u16, Value), LcuError>> + Send;
    /// Cheap check whether a client is currently reachable.
    fn is_up(&self) -> impl std::future::Future<Output = bool> + Send;
}

/// Real LCU client. Re-reads the lockfile on every call because the port and
/// password change whenever the League client restarts.
pub struct LockfileLcu {
    lockfile: PathBuf,
    http: reqwest::Client,
}

impl LockfileLcu {
    pub fn new(lockfile: PathBuf) -> Result<Self> {
        // The LCU serves a self-signed certificate on 127.0.0.1 only; this client
        // is never used for any other host.
        let http = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .timeout(std::time::Duration::from_secs(10))
            .build()?;
        Ok(Self { lockfile, http })
    }

    async fn credentials(&self) -> Result<LcuCredentials, LcuError> {
        let contents = tokio::fs::read_to_string(&self.lockfile)
            .await
            .map_err(|e| LcuError::NotRunning(format!("cannot read lockfile: {}", e.kind())))?;
        parse_lockfile(&contents).map_err(LcuError::Other)
    }
}

impl LcuApi for LockfileLcu {
    async fn get(&self, path: &str) -> Result<(u16, Value), LcuError> {
        let creds = self.credentials().await?;
        let auth = base64::engine::general_purpose::STANDARD.encode(format!("riot:{}", creds.password));
        let url = format!("https://127.0.0.1:{}{}", creds.port, path);
        let response = self
            .http
            .get(url)
            .header("Authorization", format!("Basic {auth}"))
            .header("Accept", "application/json")
            .send()
            .await
            .map_err(|e| {
                if e.is_connect() {
                    LcuError::NotRunning("connection refused".into())
                } else {
                    LcuError::Other(anyhow!("LCU request failed: {e}"))
                }
            })?;
        let status = response.status().as_u16();
        let body = response.json::<Value>().await.unwrap_or(Value::Null);
        Ok((status, body))
    }

    async fn is_up(&self) -> bool {
        matches!(self.get("/lol-summoner/v1/current-summoner").await, Ok((200, _)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_lockfile() {
        let creds = parse_lockfile("LeagueClient:1234:50000:FakePassword_123:https\r\n").unwrap();
        assert_eq!(creds.port, 50000);
        assert_eq!(creds.password, "FakePassword_123");
    }

    #[test]
    fn rejects_bad_lockfile() {
        assert!(parse_lockfile("").is_err());
        assert!(parse_lockfile("LeagueClient:1:notaport:pw:https").is_err());
        assert!(parse_lockfile("LeagueClient:1:123::https").is_err());
    }

    #[tokio::test]
    async fn missing_lockfile_is_not_running() {
        let lcu = LockfileLcu::new(PathBuf::from("Z:/definitely/missing/lockfile")).unwrap();
        assert!(matches!(lcu.get("/x").await, Err(LcuError::NotRunning(_))));
        assert!(!lcu.is_up().await);
    }
}
