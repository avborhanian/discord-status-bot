//! WebSocket endpoint that `lcu_relay` instances connect to, plus a request API
//! the rest of the bot uses to read League Client match history through them.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use relay_protocol::{LcuState, PiToRelay, RELAY_NAME_HEADER, RelayToPi};
use serde_json::Value;
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::{info, warn};

use crate::lcu_convert::{self, LcuGame, LcuGameSummary};

/// Why a relayed request failed.
#[derive(Debug)]
pub enum RelayError {
    /// No connected relay currently reports a running League client.
    NoRelay,
    /// A relay answered with a non-200 status that failover can't fix.
    Status { status: u16, error: Option<String> },
    /// Every live relay timed out, disconnected, or reported the client down.
    Unavailable,
}

impl std::fmt::Display for RelayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RelayError::NoRelay => write!(f, "no relay with a running League client is connected"),
            RelayError::Status { status, error } => {
                write!(f, "relay returned {status}: {}", error.as_deref().unwrap_or("no details"))
            }
            RelayError::Unavailable => write!(f, "all live relays timed out or reported the client down"),
        }
    }
}

impl std::error::Error for RelayError {}

struct Reply {
    status: u16,
    body: Option<Value>,
    error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct HighlightReply {
    pub success: bool,
    pub file_path: Option<String>,
    pub error: Option<String>,
}

struct RelayConn {
    conn_id: u64,
    name: String,
    tx: mpsc::UnboundedSender<String>,
    lcu_up: bool,
}

#[derive(Default)]
struct HubState {
    /// Ordered by connection time; earlier relays are preferred.
    relays: Vec<RelayConn>,
    /// request id -> (connection id, waiter)
    pending: HashMap<u64, (u64, oneshot::Sender<Reply>)>,
    /// highlight request id -> (connection id, waiter)
    pending_highlights: HashMap<u64, (u64, oneshot::Sender<HighlightReply>)>,
}

pub struct RelayHub {
    secret: String,
    timeout: Duration,
    next_id: AtomicU64,
    state: Mutex<HubState>,
    game_ended_notify: Arc<tokio::sync::Notify>,
    game_ended_tx: tokio::sync::broadcast::Sender<Option<i64>>,
}

impl std::fmt::Debug for RelayHub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print the secret.
        f.debug_struct("RelayHub").field("timeout", &self.timeout).finish_non_exhaustive()
    }
}

/// Compares secrets without short-circuiting on the first differing byte.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

impl RelayHub {
    pub fn new(secret: String, timeout: Duration) -> Arc<Self> {
        let (game_ended_tx, _) = tokio::sync::broadcast::channel(32);
        Arc::new(Self {
            secret,
            timeout,
            next_id: AtomicU64::new(1),
            state: Mutex::new(HubState::default()),
            game_ended_notify: Arc::new(tokio::sync::Notify::new()),
            game_ended_tx,
        })
    }

    #[allow(dead_code)]
    pub fn game_ended_notifier(&self) -> Arc<tokio::sync::Notify> {
        self.game_ended_notify.clone()
    }

    pub fn subscribe_game_ended(&self) -> tokio::sync::broadcast::Receiver<Option<i64>> {
        self.game_ended_tx.subscribe()
    }

    pub fn notify_game_ended(&self, game_id: Option<i64>) {
        info!("Relay reported game ended (game_id: {game_id:?})");
        let _ = self.game_ended_tx.send(game_id);
        self.game_ended_notify.notify_waiters();
    }

    pub fn router(self: &Arc<Self>) -> Router {
        Router::new()
            .route("/relay/ws", get(ws_handler))
            .with_state(self.clone())
    }

    /// Serves the relay endpoint until the listener fails.
    pub async fn serve(self: Arc<Self>, bind: &str) -> Result<()> {
        let listener = tokio::net::TcpListener::bind(bind)
            .await
            .with_context(|| format!("could not bind relay endpoint on {bind}"))?;
        info!("Relay endpoint listening on {}", listener.local_addr()?);
        axum::serve(listener, self.router()).await?;
        Ok(())
    }

    pub async fn has_live_relay(&self) -> bool {
        self.state.lock().await.relays.iter().any(|r| r.lcu_up)
    }

    async fn register(&self, name: String, tx: mpsc::UnboundedSender<String>) -> u64 {
        let conn_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.state.lock().await.relays.push(RelayConn {
            conn_id,
            name,
            tx,
            lcu_up: false,
        });
        conn_id
    }

    async fn unregister(&self, conn_id: u64) {
        let mut state = self.state.lock().await;
        state.relays.retain(|r| r.conn_id != conn_id);
        // Dropping the waiters makes in-flight requests fail over immediately.
        state.pending.retain(|_, (c, _)| *c != conn_id);
        state.pending_highlights.retain(|_, (c, _)| *c != conn_id);
    }

    async fn set_lcu(&self, conn_id: u64, up: bool) {
        let mut state = self.state.lock().await;
        if let Some(relay) = state.relays.iter_mut().find(|r| r.conn_id == conn_id) {
            if relay.lcu_up != up {
                info!("Relay '{}' reports League client {}", relay.name, if up { "up" } else { "down" });
            }
            relay.lcu_up = up;
        }
    }

    async fn complete(&self, conn_id: u64, id: u64, reply: Reply) {
        let mut state = self.state.lock().await;
        match state.pending.remove(&id) {
            Some((owner, waiter)) if owner == conn_id => {
                let _ = waiter.send(reply);
            }
            Some(entry) => {
                // A relay answered someone else's request id; put it back and ignore.
                state.pending.insert(id, entry);
                warn!("Relay connection {conn_id} answered request {id} it does not own");
            }
            None => {}
        }
    }

    async fn complete_highlight(&self, conn_id: u64, id: u64, reply: HighlightReply) {
        let mut state = self.state.lock().await;
        match state.pending_highlights.remove(&id) {
            Some((owner, waiter)) if owner == conn_id => {
                let _ = waiter.send(reply);
            }
            Some(entry) => {
                state.pending_highlights.insert(id, entry);
                warn!("Relay connection {conn_id} answered highlight {id} it does not own");
            }
            None => {}
        }
    }

    /// Sends `path` to the first live relay with GET, failing over to the next one on
    /// timeout, disconnect, or a 503 (client not running).
    pub async fn request(&self, path: &str) -> Result<Value, RelayError> {
        self.request_with_method("GET", path, None).await
    }

    /// Sends a request with the given HTTP method and optional JSON body to the first live relay,
    /// failing over to the next one on timeout, disconnect, or 503.
    pub async fn request_with_method(
        &self,
        method: &str,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, RelayError> {
        let candidates: Vec<(u64, String, mpsc::UnboundedSender<String>)> = {
            let state = self.state.lock().await;
            state
                .relays
                .iter()
                .filter(|r| r.lcu_up)
                .map(|r| (r.conn_id, r.name.clone(), r.tx.clone()))
                .collect()
        };
        if candidates.is_empty() {
            return Err(RelayError::NoRelay);
        }

        for (conn_id, name, tx) in candidates {
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            let (waiter_tx, waiter_rx) = oneshot::channel();
            self.state.lock().await.pending.insert(id, (conn_id, waiter_tx));
            let message = serde_json::to_string(&PiToRelay::Request {
                id,
                method: method.to_string(),
                path: path.to_string(),
                body: body.clone(),
            })
            .expect("request serializes");
            if tx.send(message).is_err() {
                self.state.lock().await.pending.remove(&id);
                continue;
            }
            match tokio::time::timeout(self.timeout, waiter_rx).await {
                Ok(Ok(Reply { status, body, .. })) if (200..=299).contains(&status) => {
                    return Ok(body.unwrap_or(Value::Null));
                }
                Ok(Ok(Reply { status: 503, .. })) => {
                    self.set_lcu(conn_id, false).await;
                    continue;
                }
                Ok(Ok(Reply { status, error, .. })) => return Err(RelayError::Status { status, error }),
                Ok(Err(_)) => continue, // relay disconnected mid-request
                Err(_) => {
                    warn!("Relay '{name}' timed out; trying next relay");
                    self.state.lock().await.pending.remove(&id);
                    continue;
                }
            }
        }
        Err(RelayError::Unavailable)
    }

    /// Requests download of a replay file (`.rofl`) for `game_id`.
    #[allow(dead_code)]
    pub async fn download_replay(&self, game_id: i64) -> Result<(), RelayError> {
        let path = relay_protocol::replay_download_path(game_id);
        self.request_with_method("POST", &path, None).await?;
        Ok(())
    }

    /// Fetches replay metadata / download progress for `game_id`.
    #[allow(dead_code)]
    pub async fn get_replay_metadata(&self, game_id: i64) -> Result<relay_protocol::ReplayMetadata, RelayError> {
        let path = relay_protocol::replay_metadata_path(game_id);
        let val = self.request_with_method("GET", &path, None).await?;
        serde_json::from_value(val).map_err(|e| RelayError::Status {
            status: 502,
            error: Some(format!("invalid replay metadata JSON: {e}")),
        })
    }

    /// Launches replay viewer for `game_id` in the local League game client.
    #[allow(dead_code)]
    pub async fn watch_replay(&self, game_id: i64) -> Result<(), RelayError> {
        let path = relay_protocol::replay_watch_path(game_id);
        self.request_with_method("POST", &path, None).await?;
        Ok(())
    }

    /// Returns the current gameflow phase (e.g. "None", "Lobby", "InProgress", etc.).
    #[allow(dead_code)]
    pub async fn gameflow_phase(&self) -> Result<String, RelayError> {
        let path = relay_protocol::gameflow_phase_path();
        let val = self.request_with_method("GET", path, None).await?;
        Ok(val.as_str().unwrap_or_default().to_string())
    }

    /// Triggers download of `game_id` replay and waits until download finishes or fails/times out.
    #[allow(dead_code)]
    pub async fn wait_for_replay_download(&self, game_id: i64, timeout: Duration) -> Result<(), RelayError> {
        self.download_replay(game_id).await?;
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            let meta = self.get_replay_metadata(game_id).await?;
            if meta.is_watchable() {
                return Ok(());
            }
            if meta.is_failed() {
                return Err(RelayError::Status {
                    status: 500,
                    error: Some(format!("replay download failed with state: {}", meta.state)),
                });
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Err(RelayError::Status {
            status: 408,
            error: Some(format!("replay download timed out after {:?}", timeout)),
        })
    }

    /// Fetches the details of a single game from LCU via a live relay.
    pub async fn get_game(&self, game_id: i64) -> Result<crate::lcu_convert::LcuGame, RelayError> {
        let path = relay_protocol::game_path(game_id);
        let body = self.request(&path).await?;
        serde_json::from_value(body).map_err(|e| RelayError::Status {
            status: 500,
            error: Some(format!("failed to parse game details: {e}")),
        })
    }

    /// Fetches the event timeline of a single game from LCU via a live relay.
    pub async fn game_timeline(&self, game_id: i64) -> Result<serde_json::Value, RelayError> {
        let path = relay_protocol::game_timeline_path(game_id);
        self.request(&path).await
    }

    /// Requests a random live connected relay to record a highlight video for `game_id`.
    #[allow(dead_code)]
    pub async fn request_highlight_from_random_relay(
        &self,
        game_id: i64,
        start_ms: i64,
        end_ms: i64,
        player_name: String,
        timeout: Duration,
    ) -> Result<(String, HighlightReply), RelayError> {
        use rand::seq::SliceRandom;
        let candidates: Vec<(u64, String, mpsc::UnboundedSender<String>)> = {
            let state = self.state.lock().await;
            state
                .relays
                .iter()
                .filter(|r| r.lcu_up)
                .map(|r| (r.conn_id, r.name.clone(), r.tx.clone()))
                .collect()
        };
        if candidates.is_empty() {
            return Err(RelayError::NoRelay);
        }

        let (conn_id, name, tx) = {
            let mut rng = rand::thread_rng();
            candidates
                .choose(&mut rng)
                .cloned()
                .ok_or(RelayError::NoRelay)?
        };

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (waiter_tx, waiter_rx) = oneshot::channel();
        self.state
            .lock()
            .await
            .pending_highlights
            .insert(id, (conn_id, waiter_tx));

        let message = serde_json::to_string(&PiToRelay::GenerateHighlight {
            id,
            game_id,
            start_ms,
            end_ms,
            player_name,
        })
        .expect("generate_highlight serializes");

        if tx.send(message).is_err() {
            self.state.lock().await.pending_highlights.remove(&id);
            return Err(RelayError::Unavailable);
        }

        match tokio::time::timeout(timeout, waiter_rx).await {
            Ok(Ok(reply)) => Ok((name, reply)),
            Ok(Err(_)) => Err(RelayError::Unavailable), // disconnected
            Err(_) => {
                self.state.lock().await.pending_highlights.remove(&id);
                Err(RelayError::Status {
                    status: 408,
                    error: Some(format!("Highlight generation on relay '{name}' timed out")),
                })
            }
        }
    }
}

async fn ws_handler(State(hub): State<Arc<RelayHub>>, headers: HeaderMap, ws: WebSocketUpgrade) -> Response {
    let presented = headers
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or_default();
    if !constant_time_eq(presented.as_bytes(), hub.secret.as_bytes()) {
        warn!("Rejected relay connection with a bad secret");
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let name = headers
        .get(RELAY_NAME_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unnamed")
        .chars()
        .take(64)
        .collect::<String>();
    ws.on_upgrade(move |socket| handle_socket(hub, socket, name))
}

async fn handle_socket(hub: Arc<RelayHub>, socket: WebSocket, name: String) {
    let (mut sink, mut stream) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let conn_id = hub.register(name.clone(), tx).await;
    info!("Relay '{name}' connected");

    let writer = tokio::spawn(async move {
        while let Some(text) = rx.recv().await {
            if sink.send(Message::Text(text.into())).await.is_err() {
                break;
            }
        }
    });

    while let Some(Ok(message)) = stream.next().await {
        match message {
            Message::Text(text) => match serde_json::from_str::<RelayToPi>(text.as_str()) {
                Ok(RelayToPi::Response { id, status, body, error }) => {
                    hub.complete(conn_id, id, Reply { status, body, error }).await;
                }
                Ok(RelayToPi::Status { lcu, .. }) => hub.set_lcu(conn_id, lcu == LcuState::Up).await,
                Ok(RelayToPi::GameEnded { game_id }) => hub.notify_game_ended(game_id),
                Ok(RelayToPi::HighlightResult { id, game_id: _, success, file_path, error }) => {
                    hub.complete_highlight(conn_id, id, HighlightReply { success, file_path, error }).await;
                }
                Err(e) => warn!("Relay '{name}' sent a malformed message: {e}"),
            },
            Message::Close(_) => break,
            _ => {}
        }
    }

    hub.unregister(conn_id).await;
    writer.abort();
    info!("Relay '{name}' disconnected");
}

/// Source of Mayhem match data; implemented by `RelayHub` and faked in tests.
pub trait MayhemSource: Sync {
    fn resolve_raw_puuid(&self, game_name: &str, tag_line: &str)
    -> impl std::future::Future<Output = Result<String>> + Send;
    fn recent_games(&self, raw_puuid: &str, count: u32)
    -> impl std::future::Future<Output = Result<Vec<LcuGameSummary>>> + Send;
    fn game(&self, game_id: i64) -> impl std::future::Future<Output = Result<LcuGame>> + Send;
}

impl MayhemSource for RelayHub {
    async fn resolve_raw_puuid(&self, game_name: &str, tag_line: &str) -> Result<String> {
        let body = self.request(&relay_protocol::alias_lookup_path(game_name, tag_line)).await?;
        body.get("puuid")
            .and_then(Value::as_str)
            .filter(|p| !p.is_empty())
            .map(str::to_string)
            .with_context(|| format!("no PUUID returned for {game_name}#{tag_line}"))
    }

    async fn recent_games(&self, raw_puuid: &str, count: u32) -> Result<Vec<LcuGameSummary>> {
        let end = count.saturating_sub(1);
        let body = self.request(&relay_protocol::matches_path(raw_puuid, 0, end)).await?;
        lcu_convert::parse_match_list(&body)
    }

    async fn game(&self, game_id: i64) -> Result<LcuGame> {
        self.get_game(game_id).await.map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::http::HeaderValue;
    use tokio_tungstenite::tungstenite::Message as TMessage;

    const SECRET: &str = "test-secret";

    async fn start_hub(timeout: Duration) -> (Arc<RelayHub>, String) {
        let hub = RelayHub::new(SECRET.into(), timeout);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = hub.router();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        (hub, format!("ws://{addr}/relay/ws"))
    }

    type Socket = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

    async fn connect(url: &str, secret: &str, name: &str) -> Result<Socket, tokio_tungstenite::tungstenite::Error> {
        let mut request = url.into_client_request().unwrap();
        request
            .headers_mut()
            .insert("Authorization", HeaderValue::from_str(&format!("Bearer {secret}")).unwrap());
        request.headers_mut().insert(RELAY_NAME_HEADER, HeaderValue::from_str(name).unwrap());
        tokio_tungstenite::connect_async(request).await.map(|(s, _)| s)
    }

    /// A fake relay that reports `lcu` and answers every request with `status`
    /// (or never answers when `status` is None).
    async fn fake_relay(url: &str, name: &str, lcu: LcuState, status: Option<u16>) {
        let mut socket = connect(url, SECRET, name).await.unwrap();
        let hello = RelayToPi::Status { lcu, relay: name.into(), version: "test".into() };
        socket.send(TMessage::Text(serde_json::to_string(&hello).unwrap().into())).await.unwrap();
        let name = name.to_string();
        tokio::spawn(async move {
            while let Some(Ok(TMessage::Text(text))) = socket.next().await {
                if let Ok(PiToRelay::Request { id, .. }) = serde_json::from_str(&text) {
                    if let Some(status) = status {
                        let reply = RelayToPi::Response {
                            id,
                            status,
                            body: Some(serde_json::json!({"answeredBy": name})),
                            error: None,
                        };
                        socket.send(TMessage::Text(serde_json::to_string(&reply).unwrap().into())).await.unwrap();
                    }
                }
            }
        });
    }

    async fn wait_for_relays(hub: &RelayHub, count: usize) {
        for _ in 0..100 {
            if hub.state.lock().await.relays.len() == count {
                // Give the status message a moment to land.
                tokio::time::sleep(Duration::from_millis(50)).await;
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("relays never registered");
    }

    #[tokio::test]
    async fn rejects_wrong_secret() {
        let (hub, url) = start_hub(Duration::from_secs(1)).await;
        match connect(&url, "wrong", "intruder").await {
            Err(tokio_tungstenite::tungstenite::Error::Http(response)) => assert_eq!(response.status(), 401),
            other => panic!("expected 401, got {:?}", other.map(|_| ())),
        }
        assert!(hub.state.lock().await.relays.is_empty());
    }

    #[tokio::test]
    async fn round_trips_a_request() {
        let (hub, url) = start_hub(Duration::from_secs(2)).await;
        fake_relay(&url, "alpha", LcuState::Up, Some(200)).await;
        wait_for_relays(&hub, 1).await;
        assert!(hub.has_live_relay().await);
        let body = hub.request(&relay_protocol::game_path(1)).await.unwrap();
        assert_eq!(body["answeredBy"], "alpha");
    }

    #[tokio::test]
    async fn fails_over_on_timeout_and_503() {
        let (hub, url) = start_hub(Duration::from_millis(300)).await;
        fake_relay(&url, "silent", LcuState::Up, None).await;
        wait_for_relays(&hub, 1).await;
        fake_relay(&url, "down", LcuState::Up, Some(503)).await;
        wait_for_relays(&hub, 2).await;
        fake_relay(&url, "working", LcuState::Up, Some(200)).await;
        wait_for_relays(&hub, 3).await;
        let body = hub.request(&relay_protocol::game_path(1)).await.unwrap();
        assert_eq!(body["answeredBy"], "working");
    }

    #[tokio::test]
    async fn no_live_relay_is_a_typed_error() {
        let (hub, url) = start_hub(Duration::from_secs(1)).await;
        assert!(matches!(hub.request("/x").await, Err(RelayError::NoRelay)));
        fake_relay(&url, "idle", LcuState::Down, Some(200)).await;
        wait_for_relays(&hub, 1).await;
        assert!(!hub.has_live_relay().await);
        assert!(matches!(hub.request("/x").await, Err(RelayError::NoRelay)));
    }

    #[tokio::test]
    async fn disconnect_removes_relay() {
        let (hub, url) = start_hub(Duration::from_secs(1)).await;
        let socket = connect(&url, SECRET, "brief").await.unwrap();
        wait_for_relays(&hub, 1).await;
        drop(socket);
        wait_for_relays(&hub, 0).await;
    }

    #[test]
    fn constant_time_compare() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
        assert!(!constant_time_eq(b"", b"x"));
    }

    #[tokio::test]
    async fn replay_endpoints_and_helpers() {
        let (hub, url) = start_hub(Duration::from_secs(2)).await;
        let mut socket = connect(&url, SECRET, "replay-tester").await.unwrap();
        let hello = RelayToPi::Status {
            lcu: LcuState::Up,
            relay: "replay-tester".into(),
            version: "test".into(),
        };
        socket.send(TMessage::Text(serde_json::to_string(&hello).unwrap().into())).await.unwrap();

        tokio::spawn(async move {
            while let Some(Ok(TMessage::Text(text))) = socket.next().await {
                if let Ok(PiToRelay::Request { id, method, path, .. }) = serde_json::from_str(&text) {
                    let (status, body) = if method == "POST" && path.ends_with("/download") {
                        (204, serde_json::Value::Null)
                    } else if method == "GET" && path.contains("/lol-replays/v1/rofls/") {
                        (200, serde_json::json!({
                            "gameId": 99999,
                            "state": "watch",
                            "downloadProgress": 100.0
                        }))
                    } else if method == "POST" && path.ends_with("/watch") {
                        (200, serde_json::json!({}))
                    } else if method == "GET" && path == "/lol-gameflow/v1/gameflow-phase" {
                        (200, serde_json::json!("None"))
                    } else {
                        (404, serde_json::Value::Null)
                    };
                    let reply = RelayToPi::Response {
                        id,
                        status,
                        body: Some(body),
                        error: None,
                    };
                    socket.send(TMessage::Text(serde_json::to_string(&reply).unwrap().into())).await.unwrap();
                }
            }
        });

        wait_for_relays(&hub, 1).await;
        assert!(hub.has_live_relay().await);

        hub.download_replay(99999).await.unwrap();

        let meta = hub.get_replay_metadata(99999).await.unwrap();
        assert_eq!(meta.game_id, 99999);
        assert!(meta.is_watchable());

        hub.watch_replay(99999).await.unwrap();

        let phase = hub.gameflow_phase().await.unwrap();
        assert_eq!(phase, "None");

        hub.wait_for_replay_download(99999, Duration::from_secs(2)).await.unwrap();
    }

    #[tokio::test]
    async fn random_relay_highlight_dispatch() {
        let (hub, url) = start_hub(Duration::from_secs(2)).await;
        let mut socket = connect(&url, SECRET, "pc-random").await.unwrap();
        let hello = RelayToPi::Status {
            lcu: LcuState::Up,
            relay: "pc-random".into(),
            version: "test".into(),
        };
        socket.send(TMessage::Text(serde_json::to_string(&hello).unwrap().into())).await.unwrap();

        tokio::spawn(async move {
            while let Some(Ok(TMessage::Text(text))) = socket.next().await {
                if let Ok(PiToRelay::GenerateHighlight { id, game_id, .. }) = serde_json::from_str(&text) {
                    let reply = RelayToPi::HighlightResult {
                        id,
                        game_id,
                        success: true,
                        file_path: Some("Highlights/sample.webm".into()),
                        error: None,
                    };
                    let resp_text = serde_json::to_string(&reply).unwrap();
                    socket.send(TMessage::Text(resp_text.into())).await.unwrap();
                }
            }
        });

        wait_for_relays(&hub, 1).await;
        assert!(hub.has_live_relay().await);

        let (relay_name, reply) = hub
            .request_highlight_from_random_relay(55555, 10_000, 25_000, "WarMachineZ".into(), Duration::from_secs(2))
            .await
            .unwrap();

        assert_eq!(relay_name, "pc-random");
        assert!(reply.success);
        assert_eq!(reply.file_path, Some("Highlights/sample.webm".into()));
    }
}
