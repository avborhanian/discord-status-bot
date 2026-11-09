//! `lcu_relay`: connects out to discord_status_bot over WebSocket and answers a
//! fixed allowlist of read-only League Client match-history requests.

pub mod lcu;

use std::sync::LazyLock;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use relay_protocol::{LcuState, PiToRelay, RELAY_NAME_HEADER, RelayToPi, is_allowed_request};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest, http::HeaderValue};
use tracing::{info, warn};

use crate::lcu::{LcuApi, LcuError};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Maximum LCU requests forwarded per minute by default.
pub const REQUESTS_PER_MINUTE: u32 = 120;

/// Simple token bucket: `capacity` tokens, refilled continuously over one minute.
pub struct TokenBucket {
    capacity: f64,
    tokens: f64,
    last: Instant,
}

impl TokenBucket {
    pub fn new(per_minute: u32, now: Instant) -> Self {
        Self {
            capacity: per_minute as f64,
            tokens: per_minute as f64,
            last: now,
        }
    }

    pub fn try_take(&mut self, now: Instant) -> bool {
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.capacity / 60.0).min(self.capacity);
        self.last = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// Replaces raw PUUIDs in a path so logs never contain player identifiers.
pub fn mask_path(path: &str) -> String {
    static UUID: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}").expect("valid regex")
    });
    let path = UUID.replace_all(path, "xxxxxxxx-…");
    match path.split_once("gameName=") {
        Some((head, _)) => format!("{head}gameName=…"),
        None => path.into_owned(),
    }
}

/// Validates, rate-limits and forwards a single request.
pub struct RequestHandler<L: LcuApi> {
    lcu: L,
    bucket: TokenBucket,
}

impl<L: LcuApi> RequestHandler<L> {
    pub fn new(lcu: L) -> Self {
        Self::with_rate_limit(lcu, REQUESTS_PER_MINUTE)
    }

    pub fn with_rate_limit(lcu: L, per_minute: u32) -> Self {
        Self {
            lcu,
            bucket: TokenBucket::new(per_minute, Instant::now()),
        }
    }

    pub fn lcu(&self) -> &L {
        &self.lcu
    }

    pub async fn handle(
        &mut self,
        id: u64,
        path: &str,
        method: &str,
        body: Option<&Value>,
        now: Instant,
    ) -> RelayToPi {
        let error = |status: u16, msg: &str| RelayToPi::Response {
            id,
            status,
            body: None,
            error: Some(msg.to_string()),
        };
        if !is_allowed_request(method, path) {
            warn!("Rejected disallowed request {method} {}", mask_path(path));
            return error(403, "path or method not allowed");
        }
        if !self.bucket.try_take(now) {
            warn!("Rate limit hit, rejecting {method} {}", mask_path(path));
            return error(429, "relay rate limit");
        }
        match self.lcu.request(method, path, body).await {
            Ok((status, body)) => {
                info!("{method} {} -> {}", mask_path(path), status);
                RelayToPi::Response {
                    id,
                    status,
                    body: Some(body),
                    error: None,
                }
            }
            Err(LcuError::NotRunning(why)) => error(503, &format!("league client not running ({why})")),
            Err(e) => error(502, &e.to_string()),
        }
    }
}

#[derive(Clone)]
pub struct RelayConfig {
    /// `ws://` or `wss://` URL of the bot's `/relay/ws` endpoint.
    pub server: String,
    pub secret: String,
    pub name: String,
}

async fn status_message<L: LcuApi>(handler: &RequestHandler<L>, name: &str) -> Message {
    let lcu = if handler.lcu().is_up().await { LcuState::Up } else { LcuState::Down };
    let status = RelayToPi::Status {
        lcu,
        relay: name.to_string(),
        version: VERSION.to_string(),
    };
    Message::Text(serde_json::to_string(&status).expect("status serializes"))
}

/// Runs one connection until it closes or errors.
pub async fn run_connection<L: LcuApi>(config: &RelayConfig, handler: &mut RequestHandler<L>) -> Result<()> {
    let mut request = config.server.as_str().into_client_request().context("invalid server URL")?;
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", config.secret)).context("secret has invalid characters")?,
    );
    request.headers_mut().insert(
        RELAY_NAME_HEADER,
        HeaderValue::from_str(&config.name).context("name has invalid characters")?,
    );
    let (socket, _) = tokio_tungstenite::connect_async(request)
        .await
        .context("could not connect to bot")?;
    info!("Connected to bot as '{}'", config.name);
    let (mut sink, mut stream) = socket.split();

    sink.send(status_message(handler, &config.name).await).await?;
    let mut heartbeat = tokio::time::interval(Duration::from_secs(30));
    heartbeat.tick().await;

    let mut gameflow_ticker = tokio::time::interval(Duration::from_secs(3));
    gameflow_ticker.tick().await;
    let mut last_phase: Option<String> = None;

    // Responses are produced sequentially; a channel keeps the sink single-owner.
    let (tx, mut rx) = mpsc::channel::<Message>(16);
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                sink.send(status_message(handler, &config.name).await).await?;
            }
            _ = gameflow_ticker.tick() => {
                if handler.lcu().is_up().await {
                    if let Ok((200, body)) = handler.lcu().get("/lol-gameflow/v1/gameflow-phase").await {
                        let current_phase = body.as_str().unwrap_or_default().to_string();
                        if let Some(prev) = &last_phase {
                            if (prev == "InProgress" || prev == "PreEndOfGame") && current_phase == "EndOfGame" {
                                let game_id = match handler.lcu().get("/lol-end-of-game/v1/eog-stats-block").await {
                                    Ok((200, block)) => block.get("gameId").and_then(|v| v.as_i64()),
                                    _ => None,
                                };
                                info!("Game ended on local client (game_id: {game_id:?}); notifying bot");
                                let msg = RelayToPi::GameEnded { game_id };
                                if let Ok(json) = serde_json::to_string(&msg) {
                                    let _ = tx.send(Message::Text(json)).await;
                                }
                            }
                        }
                        last_phase = Some(current_phase);
                    }
                }
            }
            Some(out) = rx.recv() => {
                sink.send(out).await?;
            }
            incoming = stream.next() => {
                let Some(incoming) = incoming else { return Ok(()) };
                match incoming? {
                    Message::Text(text) => match serde_json::from_str::<PiToRelay>(&text) {
                        Ok(PiToRelay::Request { id, method, path, body }) => {
                            let reply = handler.handle(id, &path, &method, body.as_ref(), Instant::now()).await;
                            tx.send(Message::Text(serde_json::to_string(&reply)?)).await?;
                        }
                        Err(e) => warn!("Ignoring malformed message from bot: {e}"),
                    },
                    Message::Ping(payload) => sink.send(Message::Pong(payload)).await?,
                    Message::Close(_) => return Ok(()),
                    _ => {}
                }
            }
        }
    }
}

/// Connects forever, reconnecting with exponential backoff (1 s up to 60 s).
pub async fn run_forever<L: LcuApi>(config: RelayConfig, mut handler: RequestHandler<L>) {
    let mut backoff = Duration::from_secs(1);
    loop {
        let started = Instant::now();
        match run_connection(&config, &mut handler).await {
            Ok(()) => warn!("Bot closed the connection"),
            Err(e) => warn!("Connection error: {e:#}"),
        }
        if started.elapsed() > Duration::from_secs(60) {
            backoff = Duration::from_secs(1);
        }
        info!("Reconnecting in {}s", backoff.as_secs());
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(60));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Default)]
    struct MockLcu {
        calls: Arc<AtomicUsize>,
        not_running: bool,
    }

    impl LcuApi for MockLcu {
        async fn request(
            &self,
            _method: &str,
            _path: &str,
            _body: Option<&Value>,
        ) -> Result<(u16, serde_json::Value), LcuError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.not_running {
                Err(LcuError::NotRunning("test".into()))
            } else {
                Ok((200, serde_json::json!({"ok": true})))
            }
        }
        async fn is_up(&self) -> bool {
            !self.not_running
        }
    }

    const GAME: &str = "/lol-match-history/v1/games/123";

    fn status_of(reply: &RelayToPi) -> u16 {
        match reply {
            RelayToPi::Response { status, .. } => *status,
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn disallowed_path_never_reaches_lcu() {
        let lcu = MockLcu::default();
        let calls = lcu.calls.clone();
        let mut handler = RequestHandler::new(lcu);
        let reply = handler.handle(1, "/riotclient/region-locale", "GET", None, Instant::now()).await;
        assert_eq!(status_of(&reply), 403);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn allowed_path_is_forwarded() {
        let mut handler = RequestHandler::new(MockLcu::default());
        let reply = handler.handle(9, GAME, "GET", None, Instant::now()).await;
        assert_eq!(
            reply,
            RelayToPi::Response {
                id: 9,
                status: 200,
                body: Some(serde_json::json!({"ok": true})),
                error: None
            }
        );
    }

    #[tokio::test]
    async fn allowed_post_replay_is_forwarded() {
        let mut handler = RequestHandler::new(MockLcu::default());
        let reply = handler
            .handle(10, "/lol-replays/v1/rofls/12345/download", "POST", None, Instant::now())
            .await;
        assert_eq!(
            reply,
            RelayToPi::Response {
                id: 10,
                status: 200,
                body: Some(serde_json::json!({"ok": true})),
                error: None
            }
        );
    }

    #[tokio::test]
    async fn rate_limit_kicks_in_after_capacity() {
        let lcu = MockLcu::default();
        let calls = lcu.calls.clone();
        let mut handler = RequestHandler::with_rate_limit(lcu, 10);
        let now = Instant::now();
        for i in 0..10 {
            assert_eq!(status_of(&handler.handle(i as u64, GAME, "GET", None, now).await), 200);
        }
        assert_eq!(status_of(&handler.handle(99, GAME, "GET", None, now).await), 429);
        assert_eq!(calls.load(Ordering::SeqCst), 10);
        // Six seconds later one token has refilled (10 req/min = 1 token per 6s).
        assert_eq!(status_of(&handler.handle(100, GAME, "GET", None, now + Duration::from_secs(6)).await), 200);
    }

    #[tokio::test]
    async fn client_not_running_is_503() {
        let mut handler = RequestHandler::new(MockLcu {
            not_running: true,
            ..Default::default()
        });
        assert_eq!(status_of(&handler.handle(1, GAME, "GET", None, Instant::now()).await), 503);
    }

    #[test]
    fn masks_identifiers_in_logs() {
        let masked = mask_path("/lol-match-history/v1/products/lol/0123abcd-4567-89ab-cdef-0123456789ab/matches?begIndex=0&endIndex=4");
        assert!(!masked.contains("0123abcd"));
        assert_eq!(mask_path("/lol-summoner/v1/alias/lookup?gameName=Someone&tagLine=NA1"), "/lol-summoner/v1/alias/lookup?gameName=…");
    }
}
