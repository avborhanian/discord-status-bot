//! Conversion of League Client (LCU) match-history JSON into the
//! `riven::models::match_v5::Match` model used by the rest of the bot.
//!
//! The LCU returns an older (v4-style) schema: identities are split from
//! participants, stats are nested under `stats`, and team results are the
//! strings `"Win"`/`"Fail"`.

use std::collections::HashMap;

use anyhow::{Context, Result};
use riven::models::match_v5::Match;
use serde::Deserialize;
use serde_json::{Value, json};

/// Full game returned by `GET /lol-match-history/v1/games/{gameId}`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LcuGame {
    pub game_id: i64,
    pub platform_id: String,
    pub game_creation: i64,
    /// Seconds.
    pub game_duration: i64,
    pub game_mode: String,
    pub game_type: String,
    pub game_version: String,
    pub map_id: i64,
    pub queue_id: u16,
    #[serde(default)]
    pub game_mode_mutators: Vec<String>,
    #[serde(default)]
    pub end_of_game_result: Option<String>,
    pub participants: Vec<LcuParticipant>,
    pub participant_identities: Vec<LcuIdentity>,
    pub teams: Vec<LcuTeam>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LcuParticipant {
    pub participant_id: i32,
    pub team_id: u16,
    pub champion_id: i32,
    #[serde(default)]
    pub spell1_id: i32,
    #[serde(default)]
    pub spell2_id: i32,
    pub stats: LcuStats,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct LcuStats {
    pub kills: i32,
    pub deaths: i32,
    pub assists: i32,
    pub double_kills: i32,
    pub triple_kills: i32,
    pub quadra_kills: i32,
    pub penta_kills: i32,
    pub win: bool,
    pub game_ended_in_early_surrender: bool,
    pub game_ended_in_surrender: bool,
    pub team_early_surrendered: bool,
    pub champ_level: i32,
    pub gold_earned: i32,
    pub total_damage_dealt_to_champions: i32,
    pub total_minions_killed: i32,
    pub vision_score: i32,
    pub item0: i32,
    pub item1: i32,
    pub item2: i32,
    pub item3: i32,
    pub item4: i32,
    pub item5: i32,
    pub item6: i32,
    pub player_augment1: i32,
    pub player_augment2: i32,
    pub player_augment3: i32,
    pub player_augment4: i32,
    pub player_augment5: i32,
    pub player_augment6: i32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LcuIdentity {
    pub participant_id: i32,
    pub player: LcuPlayer,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LcuPlayer {
    pub puuid: String,
    #[serde(default)]
    pub game_name: String,
    #[serde(default)]
    pub tag_line: String,
    #[serde(default)]
    pub summoner_name: String,
    #[serde(default)]
    pub profile_icon: i32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LcuTeam {
    pub team_id: u16,
    /// `"Win"` or `"Fail"`.
    pub win: String,
}

/// One entry of `GET /lol-match-history/v1/products/lol/{puuid}/matches`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LcuGameSummary {
    pub game_id: i64,
    pub queue_id: u16,
    pub game_creation: i64,
    #[serde(default)]
    pub game_type: String,
    /// Filled from the list's top-level `platformId`.
    #[serde(skip)]
    pub platform_id: String,
}

impl LcuGameSummary {
    /// Match-v5 style id, e.g. `NA1_5675903256`.
    pub fn match_id(&self) -> String {
        format!("{}_{}", self.platform_id, self.game_id)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LcuMatchList {
    #[serde(default)]
    platform_id: Option<String>,
    games: LcuMatchListGames,
}

#[derive(Debug, Deserialize)]
struct LcuMatchListGames {
    #[serde(default)]
    games: Vec<LcuGameSummary>,
}

/// Parses the body of the LCU match-list endpoint.
pub fn parse_match_list(body: &Value) -> Result<Vec<LcuGameSummary>> {
    let list = LcuMatchList::deserialize(body).context("unexpected LCU match-list shape")?;
    let platform = list.platform_id.unwrap_or_else(|| "NA1".to_string());
    Ok(list
        .games
        .games
        .into_iter()
        .map(|mut g| {
            g.platform_id = platform.clone();
            g
        })
        .collect())
}

/// Participant template with every required match-v5 field; values not known
/// from the LCU payload keep the template defaults.
const PARTICIPANT_TEMPLATE: &str = include_str!("../test_data/participant_template.json");

/// Marks matches that were converted from LCU data rather than fetched from match-v5.
pub const LCU_DATA_VERSION: &str = "lcu-1";

/// Converts an LCU game into a match-v5 `Match`.
///
/// `raw_to_encrypted` maps raw LCU PUUIDs of tracked players to the encrypted
/// PUUIDs stored in the database, so existing PUUID filters keep working.
/// Untracked players keep their raw PUUID.
pub fn to_match(game: &LcuGame, raw_to_encrypted: &HashMap<String, String>) -> Result<Match> {
    let template: Value =
        serde_json::from_str(PARTICIPANT_TEMPLATE).context("participant template is invalid JSON")?;
    let identities: HashMap<i32, &LcuPlayer> = game
        .participant_identities
        .iter()
        .map(|i| (i.participant_id, &i.player))
        .collect();

    let mut participants = Vec::with_capacity(game.participants.len());
    let mut metadata_puuids = Vec::with_capacity(game.participants.len());
    for p in &game.participants {
        let player = identities
            .get(&p.participant_id)
            .with_context(|| format!("game {} has no identity for participant {}", game.game_id, p.participant_id))?;
        let puuid = raw_to_encrypted
            .get(&player.puuid)
            .cloned()
            .unwrap_or_else(|| player.puuid.clone());
        let s = &p.stats;
        let augment = |v: i32| if v > 0 { json!(v) } else { Value::Null };

        let mut participant = template.clone();
        let fields = participant
            .as_object_mut()
            .context("participant template is not an object")?;
        for (key, value) in [
            ("puuid", json!(puuid)),
            ("participantId", json!(p.participant_id)),
            ("teamId", json!(p.team_id)),
            ("championId", json!(p.champion_id)),
            ("championName", json!("")),
            ("summoner1Id", json!(p.spell1_id)),
            ("summoner2Id", json!(p.spell2_id)),
            ("kills", json!(s.kills)),
            ("deaths", json!(s.deaths)),
            ("assists", json!(s.assists)),
            ("doubleKills", json!(s.double_kills)),
            ("tripleKills", json!(s.triple_kills)),
            ("quadraKills", json!(s.quadra_kills)),
            ("pentaKills", json!(s.penta_kills)),
            ("win", json!(s.win)),
            ("gameEndedInEarlySurrender", json!(s.game_ended_in_early_surrender)),
            ("gameEndedInSurrender", json!(s.game_ended_in_surrender)),
            ("teamEarlySurrendered", json!(s.team_early_surrendered)),
            ("champLevel", json!(s.champ_level)),
            ("goldEarned", json!(s.gold_earned)),
            ("totalDamageDealtToChampions", json!(s.total_damage_dealt_to_champions)),
            ("totalMinionsKilled", json!(s.total_minions_killed)),
            ("visionScore", json!(s.vision_score)),
            ("item0", json!(s.item0)),
            ("item1", json!(s.item1)),
            ("item2", json!(s.item2)),
            ("item3", json!(s.item3)),
            ("item4", json!(s.item4)),
            ("item5", json!(s.item5)),
            ("item6", json!(s.item6)),
            ("playerAugment1", augment(s.player_augment1)),
            ("playerAugment2", augment(s.player_augment2)),
            ("playerAugment3", augment(s.player_augment3)),
            ("playerAugment4", augment(s.player_augment4)),
            ("playerAugment5", augment(s.player_augment5)),
            ("playerAugment6", augment(s.player_augment6)),
            ("riotIdGameName", json!(player.game_name)),
            ("riotIdTagline", json!(player.tag_line)),
            ("summonerName", json!(player.summoner_name)),
            ("summonerId", json!("")),
            ("profileIcon", json!(player.profile_icon)),
            ("timePlayed", json!(game.game_duration)),
            ("individualPosition", json!("")),
            ("teamPosition", json!("")),
            ("lane", json!("")),
            ("role", json!("")),
        ] {
            fields.insert(key.to_string(), value);
        }
        metadata_puuids.push(puuid);
        participants.push(participant);
    }

    let teams: Vec<Value> = game
        .teams
        .iter()
        .map(|t| {
            let team_kills: i32 = game
                .participants
                .iter()
                .filter(|p| p.team_id == t.team_id)
                .map(|p| p.stats.kills)
                .sum();
            let none = json!({"first": false, "kills": 0});
            json!({
                "bans": [],
                "objectives": {
                    "baron": none,
                    "champion": {"first": false, "kills": team_kills},
                    "dragon": none,
                    "inhibitor": none,
                    "riftHerald": none,
                    "tower": none,
                },
                "teamId": t.team_id,
                "win": t.win.eq_ignore_ascii_case("Win"),
            })
        })
        .collect();

    let match_json = json!({
        "metadata": {
            "dataVersion": LCU_DATA_VERSION,
            "matchId": format!("{}_{}", game.platform_id, game.game_id),
            "participants": metadata_puuids,
        },
        "info": {
            "gameCreation": game.game_creation,
            "gameDuration": game.game_duration,
            "gameEndTimestamp": game.game_creation + game.game_duration * 1000,
            "gameId": game.game_id,
            "gameMode": game.game_mode,
            "gameName": format!("lcu-{}", game.game_id),
            "gameStartTimestamp": game.game_creation,
            "gameType": game.game_type,
            "gameVersion": game.game_version,
            "mapId": game.map_id,
            "gameModeMutators": game.game_mode_mutators,
            "participants": participants,
            "platformId": game.platform_id,
            "queueId": game.queue_id,
            "teams": teams,
            "tournamentCode": null,
            "endOfGameResult": game.end_of_game_result,
        },
    });

    // riven's string enums (e.g. GameMode) only deserialize from borrowed
    // strings, so go through text instead of `serde_json::from_value`.
    let text = serde_json::to_string(&match_json)?;
    serde_json::from_str(&text)
        .with_context(|| format!("converted LCU game {} is not a valid match-v5 Match", game.game_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use riven::consts::{Queue, Team};

    fn fixture() -> LcuGame {
        serde_json::from_str(include_str!("../test_data/lcu_game_mayhem.json")).expect("fixture parses")
    }

    fn raw(participant_id: u32) -> String {
        format!("00000000-0000-0000-0000-{:012x}", 0x1000 + participant_id)
    }

    #[test]
    fn converts_mayhem_fixture() {
        let m = to_match(&fixture(), &HashMap::new()).unwrap();
        assert_eq!(m.metadata.match_id, "NA1_5675903256");
        assert_eq!(m.info.queue_id, Queue(2400));
        assert_eq!(m.info.game_id, 5675903256);
        assert_eq!(m.info.participants.len(), 10);
        assert_eq!(m.metadata.participants.len(), 10);
        assert_eq!(m.info.game_end_timestamp, Some(1794125526529 + 951 * 1000));
        let blue = m.info.teams.iter().find(|t| t.team_id == Team::BLUE).unwrap();
        let red = m.info.teams.iter().find(|t| t.team_id == Team::RED).unwrap();
        assert!(blue.win);
        assert!(!red.win);

        let araam = m.info.participants.iter().find(|p| p.puuid == raw(3)).unwrap();
        assert_eq!((araam.kills, araam.deaths, araam.assists), (25, 11, 15));
        assert_eq!(araam.riot_id_game_name.as_deref(), Some("Araam"));
        assert_eq!(araam.riot_id_tagline.as_deref(), Some("NA1"));
        assert_eq!(araam.team_id, Team::BLUE);
        assert!(araam.win);
        assert_eq!(araam.player_augment1, Some(2005));
        assert!(!araam.game_ended_in_early_surrender);
    }

    #[test]
    fn maps_tracked_puuids_to_encrypted() {
        let map = HashMap::from([(raw(3), "ENCRYPTED_ARAAM".to_string())]);
        let m = to_match(&fixture(), &map).unwrap();
        assert!(m.info.participants.iter().any(|p| p.puuid == "ENCRYPTED_ARAAM"));
        assert!(m.metadata.participants.contains(&"ENCRYPTED_ARAAM".to_string()));
        assert!(m.info.participants.iter().any(|p| p.puuid == raw(1)), "untracked keep raw puuid");
        assert!(!m.info.participants.iter().any(|p| p.puuid == raw(3)));
    }

    #[test]
    fn carries_early_surrender() {
        let mut game = fixture();
        for p in &mut game.participants {
            p.stats.game_ended_in_early_surrender = true;
        }
        let m = to_match(&game, &HashMap::new()).unwrap();
        assert!(m.info.participants.iter().all(|p| p.game_ended_in_early_surrender));
    }

    #[test]
    fn converted_match_round_trips_through_json() {
        let m = to_match(&fixture(), &HashMap::new()).unwrap();
        let json = serde_json::to_string(&m).unwrap();
        let back: Match = serde_json::from_str(&json).unwrap();
        assert_eq!(back.metadata.match_id, m.metadata.match_id);
        assert_eq!(back.info.queue_id, Queue(2400));
        assert_eq!(back.info.game_mode.to_string(), "KIWI");
    }

    #[test]
    fn parses_match_list() {
        let body = json!({
            "accountId": 1,
            "platformId": "NA1",
            "games": {"gameCount": 2, "games": [
                {"gameId": 11, "queueId": 2400, "gameCreation": 1000, "gameType": "MATCHED_GAME", "participants": []},
                {"gameId": 12, "queueId": 440, "gameCreation": 2000, "gameType": "MATCHED_GAME"}
            ]}
        });
        let list = parse_match_list(&body).unwrap();
        let summary = |game_id, queue_id, game_creation| LcuGameSummary {
            game_id,
            queue_id,
            game_creation,
            game_type: "MATCHED_GAME".into(),
            platform_id: "NA1".into(),
        };
        assert_eq!(list, vec![summary(11, 2400, 1000), summary(12, 440, 2000)]);
        assert_eq!(list[0].match_id(), "NA1_11");
    }
}
