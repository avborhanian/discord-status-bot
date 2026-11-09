#![allow(clippy::result_large_err)]
//! QA harness: a throwaway "bot" WebSocket server plus the real relay code,
//! talking to the real League client on this PC.
//!
//! Usage: cargo run -p lcu_relay --example fake_pi -- <gameName> <tagLine>

use std::path::PathBuf;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use lcu_relay::lcu::LockfileLcu;
use lcu_relay::{RelayConfig, RequestHandler, run_connection};
use relay_protocol::{PiToRelay, RelayToPi, alias_lookup_path, game_path, matches_path};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

const SECRET: &str = "qa-secret";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let game_name = args.next().expect("gameName");
    let tag_line = args.next().expect("tagLine");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    println!("fake_pi listening on {addr}");

    let relay = tokio::spawn(async move {
        let config = RelayConfig {
            server: format!("ws://{addr}/relay/ws"),
            secret: SECRET.into(),
            name: "qa-relay".into(),
        };
        let mut handler = RequestHandler::new(
            LockfileLcu::new(PathBuf::from(r"C:\Riot Games\League of Legends\lockfile")).unwrap(),
        );
        run_connection(&config, &mut handler).await
    });

    let (tcp, _) = listener.accept().await?;
    let mut auth_ok = false;
    let mut name = String::new();
    let mut ws = tokio_tungstenite::accept_hdr_async(tcp, |req: &Request, resp: Response| {
        auth_ok = req.headers().get("Authorization").and_then(|v| v.to_str().ok())
            == Some(&format!("Bearer {SECRET}"));
        name = req
            .headers()
            .get(relay_protocol::RELAY_NAME_HEADER)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        Ok(resp)
    })
    .await?;
    println!("relay connected: name='{name}' bearer_ok={auth_ok}");

    let next = |ws_msg: Option<Result<Message, _>>| -> Option<RelayToPi> {
        match ws_msg? {
            Ok(Message::Text(t)) => serde_json::from_str(&t).ok(),
            _ => None,
        }
    };

    // First message must be a status heartbeat.
    let status = next(ws.next().await);
    println!("status: {status:?}");

    let request = |id: u64, path: String| PiToRelay::get(id, path);
    let send_and_wait = async |ws: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
                                   msg: PiToRelay|
           -> anyhow::Result<(u16, Option<serde_json::Value>)> {
        ws.send(Message::Text(serde_json::to_string(&msg)?)).await?;
        loop {
            let reply = tokio::time::timeout(Duration::from_secs(20), ws.next()).await?;
            if let Some(RelayToPi::Response { status, body, .. }) = next(reply) {
                return Ok((status, body));
            }
        }
    };

    let (s1, alias) = send_and_wait(&mut ws, request(1, alias_lookup_path(&game_name, &tag_line))).await?;
    let puuid = alias.as_ref().and_then(|b| b["puuid"].as_str()).unwrap_or_default().to_string();
    println!("1 alias lookup        -> {s1} (puuid resolved: {})", puuid.len() == 36);

    let (s2, list) = send_and_wait(&mut ws, request(2, matches_path(&puuid, 0, 4))).await?;
    let games = list.as_ref().map(|b| b["games"]["games"].as_array().cloned().unwrap_or_default()).unwrap_or_default();
    let queues: Vec<i64> = games.iter().filter_map(|g| g["queueId"].as_i64()).collect();
    println!("2 match list          -> {s2} ({} games, queues {queues:?})", games.len());

    let game_id = games.first().and_then(|g| g["gameId"].as_i64()).unwrap_or(0);
    let (s3, game) = send_and_wait(&mut ws, request(3, game_path(game_id))).await?;
    let participants = game.as_ref().map(|b| b["participants"].as_array().map_or(0, Vec::len)).unwrap_or(0);
    println!("3 game details        -> {s3} ({participants} participants, queue {})", game.as_ref().map_or(0, |b| b["queueId"].as_i64().unwrap_or(0)));

    let (s4, _) = send_and_wait(&mut ws, request(4, "/riotclient/region-locale".into())).await?;
    println!("4 disallowed path     -> {s4}");

    ws.close(None).await?;
    let relay_result = tokio::time::timeout(Duration::from_secs(5), relay).await;
    println!("relay connection ended cleanly: {}", matches!(relay_result, Ok(Ok(Ok(())))));
    Ok(())
}
