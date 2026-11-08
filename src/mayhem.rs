//! ARAM: Mayhem tracking through connected `lcu_relay` instances.
//!
//! Riot's public match-v5 API refuses Mayhem games, so tracked players' recent
//! histories are read through a League client (via the relay hub) instead.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::LazyLock;

use anyhow::Result;
use riven::models::match_v5::Match;
use sqlx::SqlitePool;
use tracing::{info, warn};

use crate::db;
use crate::lcu_convert;
use crate::models::MatchInfo;
use crate::relay_hub::MayhemSource;

/// ARAM: Mayhem matchmade queue (`gameMode` `KIWI`).
pub const MAYHEM_QUEUE: u16 = 2400;
/// Recent games requested per tracked player on each poll.
pub const RECENT_GAMES: u32 = 5;
/// Games created up to this long before the scan start may still end after it.
pub const CREATION_LOOKBACK_MS: i64 = 2 * 60 * 60 * 1000;
/// How often the background loop asks the relays for new Mayhem games.
pub const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(120);

/// Riot ID (`name#tag`) to raw LCU PUUID. Raw PUUIDs never change for an
/// account, so entries are kept for the life of the process.
pub type RawPuuidCache = HashMap<String, String>;

pub static RAW_PUUID_CACHE: LazyLock<tokio::sync::Mutex<RawPuuidCache>> =
    LazyLock::new(|| tokio::sync::Mutex::new(HashMap::new()));

/// Resolves raw PUUIDs for tracked players not yet in `cache` and returns a
/// raw -> encrypted PUUID map for every player that could be resolved.
///
/// `tracked` maps Riot ID (`name#tag`) to the encrypted PUUID stored in the DB.
pub async fn resolve_raw_puuids<S: MayhemSource>(
    source: &S,
    tracked: &HashMap<String, String>,
    cache: &mut RawPuuidCache,
) -> HashMap<String, String> {
    for riot_id in tracked.keys() {
        if cache.contains_key(riot_id) {
            continue;
        }
        let (name, tag) = match riot_id.split_once('#') {
            Some((name, tag)) => (name, tag),
            None => {
                info!("Tracked summoner '{riot_id}' has no tag; defaulting tag to 'NA1' for Mayhem");
                (riot_id.as_str(), "NA1")
            }
        };
        match source.resolve_raw_puuid(name, tag).await {
            Ok(raw) => {
                cache.insert(riot_id.clone(), raw.clone());
                cache.insert(format!("{name}#{tag}"), raw);
            }
            Err(e) => {
                warn!("Could not resolve raw PUUID for {riot_id}: {e:#}");
                if e.to_string().contains("429") {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
    }
    tracked
        .iter()
        .filter_map(|(riot_id, encrypted)| cache.get(riot_id).map(|raw| (raw.clone(), encrypted.clone())))
        .collect()
}

/// Mayhem games (match id -> game id) created at or after `since_ms` that appear
/// in at least two tracked players' recent histories.
pub async fn group_mayhem_games<S: MayhemSource>(
    source: &S,
    raw_puuids: &[String],
    since_ms: i64,
) -> BTreeMap<String, i64> {
    let mut seen_by: BTreeMap<String, (i64, usize)> = BTreeMap::new();
    for raw in raw_puuids {
        match source.recent_games(raw, RECENT_GAMES).await {
            Ok(games) => {
                for game in games
                    .iter()
                    .filter(|g| g.queue_id == MAYHEM_QUEUE && g.game_creation >= since_ms)
                {
                    seen_by.entry(game.match_id()).or_insert((game.game_id, 0)).1 += 1;
                }
            }
            Err(e) => {
                warn!("Could not list recent games for a tracked player: {e:#}");
                if e.to_string().contains("429") {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
    }
    seen_by
        .into_iter()
        .filter(|(_, (_, players))| *players >= 2)
        .map(|(match_id, (game_id, _))| (match_id, game_id))
        .collect()
}

/// Result of a Mayhem scan: games already stored (to re-score) and newly
/// converted games (to run through `update_match_info`).
#[derive(Default)]
pub struct MayhemScan {
    pub seen: Vec<MatchInfo>,
    pub new_matches: Vec<Match>,
}

/// Finds today's group Mayhem games, splitting them into stored and new ones.
/// Ids in `skip_ids` were already counted elsewhere in this scan.
pub async fn scan<S: MayhemSource>(
    source: &S,
    pool: &SqlitePool,
    tracked: &HashMap<String, String>,
    cache: &mut RawPuuidCache,
    start_ms: i64,
    skip_ids: &HashSet<String>,
) -> Result<MayhemScan> {
    let raw_to_encrypted = resolve_raw_puuids(source, tracked, cache).await;
    let raws: Vec<String> = raw_to_encrypted.keys().cloned().collect();
    let mut games = group_mayhem_games(source, &raws, start_ms - CREATION_LOOKBACK_MS).await;
    games.retain(|match_id, _| !skip_ids.contains(match_id));

    let ids: Vec<String> = games.keys().cloned().collect();
    let mut stored = db::fetch_seen_events(pool, &ids).await?;
    let mut result = MayhemScan::default();
    for (match_id, game_id) in games {
        if let Some(info) = stored.remove(&match_id) {
            if info.end_timestamp.timestamp_millis() >= start_ms {
                result.seen.push(info);
            }
            continue;
        }
        match source.game(game_id).await {
            Ok(game) => match lcu_convert::to_match(&game, &raw_to_encrypted) {
                Ok(converted) => result.new_matches.push(converted),
                Err(e) => warn!("Could not convert Mayhem game {match_id}: {e:#}"),
            },
            Err(e) => {
                warn!("Could not fetch Mayhem game {match_id}: {e:#}");
                if e.to_string().contains("429") {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
    }
    Ok(result)
}

/// Cheap trigger check: is there a group Mayhem game since `since_ms` that isn't stored yet?
pub async fn has_unseen<S: MayhemSource>(
    source: &S,
    pool: &SqlitePool,
    tracked: &HashMap<String, String>,
    cache: &mut RawPuuidCache,
    since_ms: i64,
) -> Result<bool> {
    let raw_to_encrypted = resolve_raw_puuids(source, tracked, cache).await;
    let raws: Vec<String> = raw_to_encrypted.keys().cloned().collect();
    let games = group_mayhem_games(source, &raws, since_ms).await;
    if games.is_empty() {
        return Ok(false);
    }
    let ids: Vec<String> = games.keys().cloned().collect();
    let stored = db::fetch_seen_events(pool, &ids).await?;
    Ok(ids.iter().any(|id| !stored.contains_key(id)))
}

#[cfg(test)]
pub mod test_support {
    //! Canned `MayhemSource` used by tests here and in `main.rs`.

    use std::collections::HashMap;
    use std::sync::Mutex;

    use anyhow::{Result, anyhow};

    use crate::lcu_convert::{LcuGame, LcuGameSummary};
    use crate::relay_hub::MayhemSource;

    pub const FIXTURE_GAME_ID: i64 = 5675903256;
    pub const FIXTURE_CREATION: i64 = 1794125526529;
    pub const RAW_RAMJACK: &str = "00000000-0000-0000-0000-000000001002";
    pub const RAW_ARAAM: &str = "00000000-0000-0000-0000-000000001003";

    pub fn fixture_game() -> LcuGame {
        serde_json::from_str(include_str!("../test_data/lcu_game_mayhem.json")).expect("fixture parses")
    }

    pub fn summary(game_id: i64, queue_id: u16, game_creation: i64) -> LcuGameSummary {
        LcuGameSummary {
            game_id,
            queue_id,
            game_creation,
            game_type: "MATCHED_GAME".into(),
            platform_id: "NA1".into(),
        }
    }

    #[derive(Default)]
    pub struct FakeSource {
        /// Riot ID -> raw PUUID
        pub aliases: HashMap<String, String>,
        /// raw PUUID -> recent games
        pub histories: HashMap<String, Vec<LcuGameSummary>>,
        pub games: HashMap<i64, LcuGame>,
        pub game_calls: Mutex<Vec<i64>>,
        pub alias_calls: Mutex<usize>,
    }

    impl FakeSource {
        /// Two tracked friends who both played the fixture Mayhem game.
        pub fn with_group_game() -> Self {
            let game = summary(FIXTURE_GAME_ID, 2400, FIXTURE_CREATION);
            Self {
                aliases: HashMap::from([
                    ("RamJack#NA1".to_string(), RAW_RAMJACK.to_string()),
                    ("Araam#NA1".to_string(), RAW_ARAAM.to_string()),
                ]),
                histories: HashMap::from([
                    (RAW_RAMJACK.to_string(), vec![game.clone()]),
                    (RAW_ARAAM.to_string(), vec![game]),
                ]),
                games: HashMap::from([(FIXTURE_GAME_ID, fixture_game())]),
                ..Default::default()
            }
        }

        pub fn game_calls(&self) -> Vec<i64> {
            self.game_calls.lock().unwrap().clone()
        }
    }

    impl MayhemSource for FakeSource {
        async fn resolve_raw_puuid(&self, game_name: &str, tag_line: &str) -> Result<String> {
            *self.alias_calls.lock().unwrap() += 1;
            self.aliases
                .get(&format!("{game_name}#{tag_line}"))
                .cloned()
                .ok_or_else(|| anyhow!("unknown alias"))
        }
        async fn recent_games(&self, raw_puuid: &str, count: u32) -> Result<Vec<LcuGameSummary>> {
            let mut games = self.histories.get(raw_puuid).cloned().unwrap_or_default();
            games.truncate(count as usize);
            Ok(games)
        }
        async fn game(&self, game_id: i64) -> Result<LcuGame> {
            self.game_calls.lock().unwrap().push(game_id);
            self.games.get(&game_id).cloned().ok_or_else(|| anyhow!("no such game"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;

    fn tracked() -> HashMap<String, String> {
        HashMap::from([
            ("RamJack#NA1".to_string(), "ENC_RAMJACK".to_string()),
            ("Araam#NA1".to_string(), "ENC_ARAAM".to_string()),
        ])
    }

    async fn pool() -> SqlitePool {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::query(
            "CREATE TABLE MATCH (Id TEXT PRIMARY KEY, QUEUE_ID VARCHAR(255) NOT NULL, Win BOOLEAN, END_TIMESTAMP INTEGER, MatchInfo BLOB)",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool
    }

    const START: i64 = FIXTURE_CREATION - 60 * 60 * 1000;

    #[tokio::test]
    async fn resolves_and_caches_raw_puuids() {
        let source = FakeSource::with_group_game();
        let mut cache = RawPuuidCache::new();
        let mut tracked = tracked();
        tracked.insert("Ghost#NA1".into(), "ENC_GHOST".into()); // alias lookup fails
        let map = resolve_raw_puuids(&source, &tracked, &mut cache).await;
        assert_eq!(map.get(RAW_ARAAM).map(String::as_str), Some("ENC_ARAAM"));
        assert_eq!(map.get(RAW_RAMJACK).map(String::as_str), Some("ENC_RAMJACK"));
        assert_eq!(map.len(), 2);
        let calls_after_first = *source.alias_calls.lock().unwrap();
        resolve_raw_puuids(&source, &tracked, &mut cache).await;
        // Only the unresolved player is looked up again.
        assert_eq!(*source.alias_calls.lock().unwrap(), calls_after_first + 1);
    }

    #[tokio::test]
    async fn resolves_summoner_without_tag_using_na1_fallback() {
        let source = FakeSource::with_group_game();
        let mut cache = RawPuuidCache::new();
        // Summoner names stored without '#TAG' in legacy registrations
        let tracked = HashMap::from([
            ("Araam".to_string(), "ENC_ARAAM".to_string()),
            ("RamJack".to_string(), "ENC_RAMJACK".to_string()),
        ]);
        let map = resolve_raw_puuids(&source, &tracked, &mut cache).await;
        assert_eq!(map.get(RAW_ARAAM).map(String::as_str), Some("ENC_ARAAM"));
        assert_eq!(map.get(RAW_RAMJACK).map(String::as_str), Some("ENC_RAMJACK"));
        assert_eq!(map.len(), 2);
    }

    #[tokio::test]
    async fn only_group_mayhem_games_since_start_count() {
        let mut source = FakeSource::with_group_game();
        let araam = source.histories.get_mut(RAW_ARAAM).unwrap();
        araam.push(summary(1, 2400, FIXTURE_CREATION)); // solo Mayhem: one tracked player
        araam.push(summary(2, 440, FIXTURE_CREATION)); // Flex
        source.histories.get_mut(RAW_RAMJACK).unwrap().push(summary(2, 440, FIXTURE_CREATION));
        let old = summary(3, 2400, START - CREATION_LOOKBACK_MS - 1); // too old
        source.histories.get_mut(RAW_ARAAM).unwrap().push(old.clone());
        source.histories.get_mut(RAW_RAMJACK).unwrap().push(old);

        let raws = vec![RAW_ARAAM.to_string(), RAW_RAMJACK.to_string()];
        let games = group_mayhem_games(&source, &raws, START - CREATION_LOOKBACK_MS).await;
        assert_eq!(games, BTreeMap::from([(format!("NA1_{FIXTURE_GAME_ID}"), FIXTURE_GAME_ID)]));
    }

    #[tokio::test]
    async fn scan_converts_new_group_game_with_encrypted_puuids() {
        let source = FakeSource::with_group_game();
        let pool = pool().await;
        let mut cache = RawPuuidCache::new();
        let result = scan(&source, &pool, &tracked(), &mut cache, START, &HashSet::new()).await.unwrap();
        assert!(result.seen.is_empty());
        assert_eq!(result.new_matches.len(), 1);
        let m = &result.new_matches[0];
        assert_eq!(m.metadata.match_id, format!("NA1_{FIXTURE_GAME_ID}"));
        let ids: Vec<&str> = m.info.participants.iter().map(|p| p.puuid.as_str()).collect();
        assert!(ids.contains(&"ENC_ARAAM") && ids.contains(&"ENC_RAMJACK"));
    }

    #[tokio::test]
    async fn scan_reuses_stored_games_and_honours_skip_ids() {
        let source = FakeSource::with_group_game();
        let pool = pool().await;
        let mut cache = RawPuuidCache::new();
        let id = format!("NA1_{FIXTURE_GAME_ID}");
        let stored = crate::lcu_convert::to_match(&fixture_game(), &HashMap::new()).unwrap();
        db::update_match(&pool, &id, 2400, true, stored.info.game_end_timestamp.unwrap(), &stored)
            .await
            .unwrap();

        let result = scan(&source, &pool, &tracked(), &mut cache, START, &HashSet::new()).await.unwrap();
        assert_eq!(result.seen.len(), 1);
        assert!(result.new_matches.is_empty());
        assert!(source.game_calls().is_empty(), "stored game must not be refetched");

        let skipped = scan(&source, &pool, &tracked(), &mut cache, START, &HashSet::from([id])).await.unwrap();
        assert!(skipped.seen.is_empty() && skipped.new_matches.is_empty());
    }

    #[tokio::test]
    async fn scan_ignores_stored_game_that_ended_before_start() {
        let source = FakeSource::with_group_game();
        let pool = pool().await;
        let mut cache = RawPuuidCache::new();
        let id = format!("NA1_{FIXTURE_GAME_ID}");
        let stored = crate::lcu_convert::to_match(&fixture_game(), &HashMap::new()).unwrap();
        let end = stored.info.game_end_timestamp.unwrap();
        db::update_match(&pool, &id, 2400, true, end, &stored).await.unwrap();
        let result = scan(&source, &pool, &tracked(), &mut cache, end + 1, &HashSet::new()).await.unwrap();
        assert!(result.seen.is_empty());
    }

    #[tokio::test]
    async fn has_unseen_detects_new_group_games_only() {
        let source = FakeSource::with_group_game();
        let pool = pool().await;
        let mut cache = RawPuuidCache::new();
        assert!(has_unseen(&source, &pool, &tracked(), &mut cache, START).await.unwrap());
        // Created before `since`: not a trigger.
        assert!(!has_unseen(&source, &pool, &tracked(), &mut cache, FIXTURE_CREATION + 1).await.unwrap());

        let stored = crate::lcu_convert::to_match(&fixture_game(), &HashMap::new()).unwrap();
        db::update_match(&pool, &stored.metadata.match_id, 2400, true, stored.info.game_end_timestamp.unwrap(), &stored)
            .await
            .unwrap();
        assert!(!has_unseen(&source, &pool, &tracked(), &mut cache, START).await.unwrap());
    }
}
