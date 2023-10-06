use riven::models::match_v5::Match;
use serde_derive::Deserialize;
use serde_derive::Serialize;

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiscordLogin {
    pub user_id: String,
    pub token: String,
}

pub struct Score {
    pub wins: u32,
    pub games: u32,
    pub warmup: u32,
}

pub struct MatchInfo {
    pub id: String,
    pub queue_id: String,
    pub win: bool,
    pub end_timestamp: chrono::NaiveDateTime,
    pub match_info: Option<Match>,
}
