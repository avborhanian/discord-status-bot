//! Messages exchanged between the bot (the "Pi") and `lcu_relay`, plus the
//! allowlist of League Client (LCU) paths a relay is permitted to forward.

use serde::{Deserialize, Serialize};

/// Header carrying the relay's display name on the WebSocket upgrade request.
pub const RELAY_NAME_HEADER: &str = "X-Relay-Name";

/// Messages sent from the bot to a relay.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PiToRelay {
    Request {
        id: u64,
        #[serde(default = "default_method", skip_serializing_if = "is_default_method")]
        method: String,
        path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        body: Option<serde_json::Value>,
    },
    /// Directs a relay to record a highlight video for a specific match.
    GenerateHighlight {
        id: u64,
        game_id: i64,
        start_ms: i64,
        end_ms: i64,
        player_name: String,
    },
}

impl PiToRelay {
    /// Creates a GET request.
    pub fn get(id: u64, path: impl Into<String>) -> Self {
        Self::Request {
            id,
            method: "GET".to_string(),
            path: path.into(),
            body: None,
        }
    }

    /// Creates a POST request with an optional JSON body.
    pub fn post(id: u64, path: impl Into<String>, body: Option<serde_json::Value>) -> Self {
        Self::Request {
            id,
            method: "POST".to_string(),
            path: path.into(),
            body,
        }
    }
}

fn default_method() -> String {
    "GET".to_string()
}

fn is_default_method(method: &str) -> bool {
    method == "GET"
}

/// Whether the relay can currently reach a running League client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LcuState {
    Up,
    Down,
}

/// Messages sent from a relay to the bot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RelayToPi {
    Response {
        id: u64,
        status: u16,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        body: Option<serde_json::Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    Status {
        lcu: LcuState,
        relay: String,
        version: String,
    },
    /// Notification that a game ended on this relay host's client.
    GameEnded {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        game_id: Option<i64>,
    },
    /// Result of a highlight recording request on this relay.
    HighlightResult {
        id: u64,
        game_id: i64,
        success: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        file_path: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
}

/// Path listing a player's recent LoL games (raw, unencrypted PUUID).
pub fn matches_path(raw_puuid: &str, beg_index: u32, end_index: u32) -> String {
    format!("/lol-match-history/v1/products/lol/{raw_puuid}/matches?begIndex={beg_index}&endIndex={end_index}")
}

/// Path returning the full details of a single game.
pub fn game_path(game_id: i64) -> String {
    format!("/lol-match-history/v1/games/{game_id}")
}

/// Path resolving a Riot ID to its raw PUUID.
pub fn alias_lookup_path(game_name: &str, tag_line: &str) -> String {
    format!(
        "/lol-summoner/v1/alias/lookup?gameName={}&tagLine={}",
        urlencoding::encode(game_name),
        urlencoding::encode(tag_line)
    )
}

/// Path triggering download of a replay (.rofl) file (POST).
pub fn replay_download_path(game_id: i64) -> String {
    format!("/lol-replays/v1/rofls/{game_id}/download")
}

/// Path querying metadata / download status of a replay file (GET).
pub fn replay_metadata_path(game_id: i64) -> String {
    format!("/lol-replays/v1/rofls/{game_id}")
}

/// Path launching replay playback in the League game client (POST).
pub fn replay_watch_path(game_id: i64) -> String {
    format!("/lol-replays/v1/rofls/{game_id}/watch")
}

/// Path querying current gameflow phase (GET).
pub fn gameflow_phase_path() -> &'static str {
    "/lol-gameflow/v1/gameflow-phase"
}

/// Metadata and download status returned by `GET /lol-replays/v1/rofls/{game_id}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplayMetadata {
    pub game_id: i64,
    pub state: String,
    #[serde(default)]
    pub download_progress: Option<f64>,
}

impl ReplayMetadata {
    pub fn is_watchable(&self) -> bool {
        self.state.eq_ignore_ascii_case("watch")
    }

    pub fn is_downloading(&self) -> bool {
        self.state.eq_ignore_ascii_case("downloading")
            || self.state.eq_ignore_ascii_case("inQueue")
    }

    pub fn is_failed(&self) -> bool {
        self.state.eq_ignore_ascii_case("failed")
            || self.state.eq_ignore_ascii_case("lost")
    }
}

/// Returns true only for the allowed LCU request method and path combinations a relay may forward.
///
/// Anything else, including extra query keys, path traversal, or unlisted methods/routes, is rejected.
pub fn is_allowed_request(method: &str, path: &str) -> bool {
    use std::sync::LazyLock;
    static MATCHES: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(
            r"^/lol-match-history/v1/products/lol/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/matches$",
        )
        .expect("valid regex")
    });
    static GAME: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r"^/lol-match-history/v1/games/[0-9]{1,20}$").expect("valid regex")
    });
    static REPLAY_ROFL: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r"^/lol-replays/v1/rofls/[0-9]{1,20}$").expect("valid regex")
    });
    static REPLAY_DOWNLOAD: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r"^/lol-replays/v1/rofls/[0-9]{1,20}/download$").expect("valid regex")
    });
    static REPLAY_WATCH: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r"^/lol-replays/v1/rofls/[0-9]{1,20}/watch$").expect("valid regex")
    });
    static NUMBER: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"^[0-9]{1,4}$").expect("valid regex"));
    static ALIAS_VALUE: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"^[A-Za-z0-9%._~+-]{1,96}$").expect("valid regex"));

    if path.contains("..") || path.contains('#') || path.contains('\\') {
        return false;
    }
    let (route, query) = match path.split_once('?') {
        Some((route, query)) => (route, Some(query)),
        None => (path, None),
    };

    match method.to_ascii_uppercase().as_str() {
        "GET" => {
            let pairs: Vec<(&str, &str)> = match query {
                None => Vec::new(),
                Some(q) => {
                    let mut pairs = Vec::new();
                    for part in q.split('&') {
                        match part.split_once('=') {
                            Some(pair) => pairs.push(pair),
                            None => return false,
                        }
                    }
                    pairs
                }
            };
            let keys_are = |expected: &[&str]| {
                pairs.len() == expected.len()
                    && expected
                        .iter()
                        .all(|k| pairs.iter().filter(|(pk, _)| pk == k).count() == 1)
            };

            if MATCHES.is_match(route) {
                keys_are(&["begIndex", "endIndex"]) && pairs.iter().all(|(_, v)| NUMBER.is_match(v))
            } else if GAME.is_match(route) {
                query.is_none()
            } else if route == "/lol-summoner/v1/alias/lookup" {
                keys_are(&["gameName", "tagLine"]) && pairs.iter().all(|(_, v)| ALIAS_VALUE.is_match(v))
            } else if REPLAY_ROFL.is_match(route) {
                query.is_none()
            } else if route == "/lol-gameflow/v1/gameflow-phase" {
                query.is_none()
            } else {
                false
            }
        }
        "POST" => {
            if query.is_some() {
                return false;
            }
            REPLAY_DOWNLOAD.is_match(route) || REPLAY_WATCH.is_match(route)
        }
        _ => false,
    }
}

/// Returns true only for the read-only LCU requests a relay may forward.
pub fn is_allowed_path(path: &str) -> bool {
    is_allowed_request("GET", path)
}

#[cfg(test)]
mod tests {
    use super::*;

    const RAW: &str = "0123abcd-4567-89ab-cdef-0123456789ab";

    #[test]
    fn builders_produce_allowed_paths() {
        assert!(is_allowed_path(&matches_path(RAW, 0, 4)));
        assert!(is_allowed_path(&game_path(5675903256)));
        assert!(is_allowed_path(&alias_lookup_path("SomePlayer", "NA1")));
        assert!(is_allowed_path(&alias_lookup_path("Raizer ffect", "NA1")));
        assert!(is_allowed_path(&alias_lookup_path("Raizeŕffect", "NA1")));
        assert!(is_allowed_request("POST", &replay_download_path(5675903256)));
        assert!(is_allowed_request("GET", &replay_metadata_path(5675903256)));
        assert!(is_allowed_request("POST", &replay_watch_path(5675903256)));
        assert!(is_allowed_request("GET", gameflow_phase_path()));
    }

    #[test]
    fn replay_method_and_query_validation() {
        let game_id = 123456789;
        assert!(is_allowed_request("POST", &replay_download_path(game_id)));
        assert!(!is_allowed_request("GET", &replay_download_path(game_id)));
        assert!(!is_allowed_request("POST", &format!("{}?x=1", replay_download_path(game_id))));

        assert!(is_allowed_request("GET", &replay_metadata_path(game_id)));
        assert!(!is_allowed_request("POST", &replay_metadata_path(game_id)));
        assert!(!is_allowed_request("GET", &format!("{}?x=1", replay_metadata_path(game_id))));

        assert!(is_allowed_request("POST", &replay_watch_path(game_id)));
        assert!(!is_allowed_request("GET", &replay_watch_path(game_id)));

        assert!(is_allowed_request("GET", gameflow_phase_path()));
        assert!(!is_allowed_request("POST", gameflow_phase_path()));
    }

    #[test]
    fn replay_metadata_helpers() {
        let watch: ReplayMetadata = serde_json::from_str(r#"{"gameId": 123, "state": "watch"}"#).unwrap();
        assert!(watch.is_watchable());
        assert!(!watch.is_downloading());
        assert!(!watch.is_failed());

        let dl: ReplayMetadata = serde_json::from_str(r#"{"gameId": 123, "state": "downloading", "downloadProgress": 45.2}"#).unwrap();
        assert!(!dl.is_watchable());
        assert!(dl.is_downloading());
        assert_eq!(dl.download_progress, Some(45.2));

        let queue: ReplayMetadata = serde_json::from_str(r#"{"gameId": 123, "state": "inQueue"}"#).unwrap();
        assert!(queue.is_downloading());

        let failed: ReplayMetadata = serde_json::from_str(r#"{"gameId": 123, "state": "failed"}"#).unwrap();
        assert!(failed.is_failed());
    }

    #[test]
    fn rejects_other_endpoints() {
        for path in [
            "/lol-login/v1/session",
            "/riotclient/region-locale",
            "/lol-rso-auth/v1/authorization/access-token",
            "/lol-match-history/v1/products/lol/current-summoner/matches",
            "/lol-match-history/v1/games/123/extra",
            "/lol-match-history/v1/games/1/../../riotclient/region-locale",
            "/lol-match-history/v1/game-timelines/123",
            "",
            "/",
        ] {
            assert!(!is_allowed_path(path), "should reject {path}");
        }
    }

    #[test]
    fn rejects_encrypted_puuid_and_bad_queries() {
        let encrypted = "P3HwOOx99zBCUYB8hio73VrgN3nsyNyctadDAiOq5SW8SFRczzQyHJa6tB3uAuZxyLuTqixawhaH1w";
        assert!(!is_allowed_path(&format!(
            "/lol-match-history/v1/products/lol/{encrypted}/matches?begIndex=0&endIndex=4"
        )));
        assert!(!is_allowed_path(&format!("/lol-match-history/v1/products/lol/{RAW}/matches")));
        assert!(!is_allowed_path(&format!(
            "/lol-match-history/v1/products/lol/{RAW}/matches?begIndex=0&endIndex=4&x=1"
        )));
        assert!(!is_allowed_path(&format!(
            "/lol-match-history/v1/products/lol/{RAW}/matches?begIndex=0&begIndex=4"
        )));
        assert!(!is_allowed_path(&format!(
            "/lol-match-history/v1/products/lol/{RAW}/matches?begIndex=a&endIndex=4"
        )));
        assert!(!is_allowed_path("/lol-match-history/v1/games/123?x=1"));
        assert!(!is_allowed_path("/lol-summoner/v1/alias/lookup?gameName=a"));
        assert!(!is_allowed_path("/lol-summoner/v1/alias/lookup?gameName=a/b&tagLine=NA1"));
        assert!(!is_allowed_path("/lol-summoner/v1/alias/lookup?gameName=a&tagLine=NA1#x"));
    }

    #[test]
    fn messages_round_trip() {
        let request = PiToRelay::Request {
            id: 42,
            method: "GET".into(),
            path: game_path(1),
            body: None,
        };
        let json = serde_json::to_string(&request).unwrap();
        // Method "GET" and body None are skipped for backward compatibility
        assert_eq!(json, r#"{"type":"request","id":42,"path":"/lol-match-history/v1/games/1"}"#);
        assert_eq!(serde_json::from_str::<PiToRelay>(&json).unwrap(), request);

        // Legacy message without method or body parses as GET and None
        let legacy = r#"{"type":"request","id":42,"path":"/lol-match-history/v1/games/1"}"#;
        assert_eq!(serde_json::from_str::<PiToRelay>(legacy).unwrap(), request);

        // POST request serializes method and optional body
        let post_req = PiToRelay::Request {
            id: 43,
            method: "POST".into(),
            path: replay_download_path(1),
            body: Some(serde_json::json!({"opt": true})),
        };
        let post_json = serde_json::to_string(&post_req).unwrap();
        assert!(post_json.contains(r#""method":"POST""#));
        assert!(post_json.contains(r#""body":{"opt":true}"#));
        assert_eq!(serde_json::from_str::<PiToRelay>(&post_json).unwrap(), post_req);

        let ok = RelayToPi::Response {
            id: 42,
            status: 200,
            body: Some(serde_json::json!({"gameId": 1})),
            error: None,
        };
        let json = serde_json::to_string(&ok).unwrap();
        assert!(!json.contains("error"));
        assert_eq!(serde_json::from_str::<RelayToPi>(&json).unwrap(), ok);

        let err = RelayToPi::Response {
            id: 7,
            status: 503,
            body: None,
            error: Some("league client not running".into()),
        };
        let json = serde_json::to_string(&err).unwrap();
        assert_eq!(serde_json::from_str::<RelayToPi>(&json).unwrap(), err);

        let status = RelayToPi::Status {
            lcu: LcuState::Up,
            relay: "araam-pc".into(),
            version: "0.1.0".into(),
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains(r#""lcu":"up""#));
        assert_eq!(serde_json::from_str::<RelayToPi>(&json).unwrap(), status);

        let ended = RelayToPi::GameEnded { game_id: Some(56789) };
        let json = serde_json::to_string(&ended).unwrap();
        assert_eq!(json, r#"{"type":"game_ended","game_id":56789}"#);
        assert_eq!(serde_json::from_str::<RelayToPi>(&json).unwrap(), ended);

        let ended_none = RelayToPi::GameEnded { game_id: None };
        let json = serde_json::to_string(&ended_none).unwrap();
        assert_eq!(json, r#"{"type":"game_ended"}"#);
        assert_eq!(serde_json::from_str::<RelayToPi>(&json).unwrap(), ended_none);

        let highlight_req = PiToRelay::GenerateHighlight {
            id: 100,
            game_id: 56789,
            start_ms: 120_500,
            end_ms: 145_000,
            player_name: "WarMachineZ".into(),
        };
        let json = serde_json::to_string(&highlight_req).unwrap();
        assert_eq!(json, r#"{"type":"generate_highlight","id":100,"game_id":56789,"start_ms":120500,"end_ms":145000,"player_name":"WarMachineZ"}"#);
        assert_eq!(serde_json::from_str::<PiToRelay>(&json).unwrap(), highlight_req);

        let res = RelayToPi::HighlightResult {
            id: 100,
            game_id: 56789,
            success: true,
            file_path: Some("Highlights/clip.webm".into()),
            error: None,
        };
        let json = serde_json::to_string(&res).unwrap();
        assert_eq!(json, r#"{"type":"highlight_result","id":100,"game_id":56789,"success":true,"file_path":"Highlights/clip.webm"}"#);
        assert_eq!(serde_json::from_str::<RelayToPi>(&json).unwrap(), res);
    }
}
