//! Records game replay clips using the local League Client (LCU) and the
//! Game Client Replay API on port 2999.

use std::time::Duration;

use anyhow::{anyhow, Result};
use serde_json::json;
use tracing::{info, warn};

use crate::lcu::LcuApi;

/// Checks that `EnableReplayApi=1` is set in League's `game.cfg` so port 2999 is accessible during replays.
fn ensure_replay_api_enabled() {
    let cfg_path = std::path::Path::new(r"C:\Riot Games\League of Legends\Config\game.cfg");
    if cfg_path.exists() {
        if let Ok(content) = std::fs::read_to_string(cfg_path) {
            if content.contains("EnableReplayApi=0") {
                let updated = content.replace("EnableReplayApi=0", "EnableReplayApi=1");
                let _ = std::fs::write(cfg_path, updated);
                info!("Automatically enabled Replay API in game.cfg (EnableReplayApi=1)");
            } else if !content.contains("EnableReplayApi") {
                let updated = format!("{content}\n[General]\nEnableReplayApi=1\n");
                let _ = std::fs::write(cfg_path, updated);
                info!("Added EnableReplayApi=1 to game.cfg");
            }
        }
    }
}

/// Downloads a replay via LCU, launches it into the game client, and records the
/// requested time window using the Game Client Replay API on port 2999.
pub async fn record_highlight<L: LcuApi>(
    lcu: &L,
    game_id: i64,
    start_ms: i64,
    end_ms: i64,
    player_name: &str,
) -> Result<String> {
    ensure_replay_api_enabled();
    let start_time = (start_ms as f64 / 1000.0).max(0.0);
    let end_time = (end_ms as f64 / 1000.0).max(start_time + 1.0);
    info!(
        "Starting highlight recording for game {game_id} (player: {player_name}, {start_time:.1}s - {end_time:.1}s)"
    );

    // 1. Verify gameflow phase on LCU (never interrupt an active match)
    if let Ok((200, phase_val)) = lcu.get("/lol-gameflow/v1/gameflow-phase").await {
        let phase = phase_val.as_str().unwrap_or_default();
        if phase == "InProgress" || phase == "ChampSelect" {
            return Err(anyhow!(
                "Cannot record replay: local client is currently in-game ({phase})"
            ));
        }
    }

    // 2. Initialize replay metadata on LCU (required if replay hasn't been viewed in client yet)
    let create_path = relay_protocol::replay_create_metadata_path(game_id);
    let empty_body = json!({});
    if let Err(e) = lcu.post(&create_path, Some(&empty_body)).await {
        warn!("Replay metadata create attempt returned error: {e}");
    }

    // 3. Trigger replay download on LCU
    let dl_path = relay_protocol::replay_download_path(game_id);
    let (dl_status, _) = lcu
        .post(&dl_path, Some(&empty_body))
        .await
        .map_err(|e| anyhow!("Failed to trigger replay download: {e}"))?;
    info!("Replay download requested (status {dl_status})");

    // 4. Wait for replay to be downloaded (state == "watch")
    let meta_path = relay_protocol::replay_metadata_path(game_id);
    let mut downloaded = false;
    let poll_start = std::time::Instant::now();
    while poll_start.elapsed() < Duration::from_secs(60) {
        if let Ok((200, val)) = lcu.get(&meta_path).await {
            if let Ok(meta) = serde_json::from_value::<relay_protocol::ReplayMetadata>(val) {
                if meta.is_watchable() {
                    downloaded = true;
                    break;
                }
                if meta.is_failed() {
                    return Err(anyhow!("Replay download failed (state: {})", meta.state));
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(1000)).await;
    }
    if !downloaded {
        return Err(anyhow!("Replay download timed out after 60s"));
    }
    info!("Replay is watchable. Launching replay viewer...");

    // 5. Launch replay
    let watch_path = relay_protocol::replay_watch_path(game_id);
    let (watch_status, _) = lcu
        .post(&watch_path, Some(&empty_body))
        .await
        .map_err(|e| anyhow!("Failed to launch replay: {e}"))?;
    info!("Replay launch sent (status {watch_status})");

    // 5. Connect to Game Client Replay API on port 2999
    let replay_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_secs(10))
        .build()?;

    let mut game_ready = false;
    let launch_start = std::time::Instant::now();
    while launch_start.elapsed() < Duration::from_secs(60) {
        if let Ok(res) = replay_client
            .get("https://127.0.0.1:2999/replay/playback")
            .send()
            .await
        {
            if res.status().is_success() {
                game_ready = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    if !game_ready {
        return Err(anyhow!(
            "Game client replay API did not become ready within 60s"
        ));
    }
    info!("Game client replay API is ready on port 2999!");

    // 6. Configure playback position
    if let Ok(playback_res) = replay_client
        .get("https://127.0.0.1:2999/replay/playback")
        .send()
        .await
    {
        if let Ok(mut playback) = playback_res.json::<serde_json::Value>().await {
            playback["time"] = json!(start_time);
            playback["paused"] = json!(false);
            let _ = replay_client
                .post("https://127.0.0.1:2999/replay/playback")
                .json(&playback)
                .send()
                .await;
            info!("Playback seeked to {start_time:.1}s; waiting 5s for directed camera...");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    // 7. Configure render settings
    if let Ok(render_res) = replay_client
        .get("https://127.0.0.1:2999/replay/render")
        .send()
        .await
    {
        if let Ok(mut render) = render_res.json::<serde_json::Value>().await {
            if let Some(obj) = render.as_object_mut() {
                obj.insert("selectionName".to_string(), json!(player_name));
                if let Some(pos) = obj.get_mut("cameraPosition").and_then(|p| p.as_object_mut()) {
                    pos.insert("y".to_string(), json!(2319.708984375));
                }
                obj.insert("farClip".to_string(), json!(24730.0));
                obj.insert("characters".to_string(), json!(true));
                obj.insert("interfaceAll".to_string(), json!(false));
                obj.insert("cameraAttached".to_string(), json!(true));
                obj.insert("outlineHover".to_string(), json!(false));
                obj.insert("outlineSelect".to_string(), json!(true));
            }
            let _ = replay_client
                .post("https://127.0.0.1:2999/replay/render")
                .json(&render)
                .send()
                .await;
        }
    }

    // 8. Start recording
    let mut recording_val = json!({
        "recording": true,
        "lossless": false,
        "height": 720
    });
    if let Ok(rec_res) = replay_client
        .get("https://127.0.0.1:2999/replay/recording")
        .send()
        .await
    {
        if let Ok(mut rec) = rec_res.json::<serde_json::Value>().await {
            rec["recording"] = json!(true);
            rec["lossless"] = json!(false);
            rec["height"] = json!(720);
            recording_val = rec;
        }
    }
    let _ = replay_client
        .post("https://127.0.0.1:2999/replay/recording")
        .json(&recording_val)
        .send()
        .await;

    let record_duration = (end_time - start_time).max(1.0).min(60.0);
    info!("Recording started. Waiting {record_duration:.1}s...");
    tokio::time::sleep(Duration::from_secs_f64(record_duration)).await;

    // 9. Stop recording
    recording_val["recording"] = json!(false);
    let stop_res = replay_client
        .post("https://127.0.0.1:2999/replay/recording")
        .json(&recording_val)
        .send()
        .await;
    let final_rec: serde_json::Value = match stop_res {
        Ok(r) => r.json().await.unwrap_or(recording_val),
        Err(e) => {
            warn!("Error stopping recording: {e}");
            recording_val
        }
    };
    let out_path = final_rec
        .get("path")
        .and_then(|p| p.as_str())
        .unwrap_or("Documents/League of Legends/Highlights")
        .to_string();

    info!("Highlight recording finished! Output path: {out_path}");
    Ok(out_path)
}
