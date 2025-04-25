use chrono::Utc;
use riven::models::match_v5::Match;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Score {
    pub wins: u32,
    pub games: u32,
    pub warmup: u32,
    pub top_finishes: u32,
    pub bottom_finishes: u32,
}

#[allow(dead_code)]
pub struct MatchInfo {
    pub id: String,
    pub queue_id: String,
    pub win: bool,
    pub end_timestamp: chrono::DateTime<Utc>,
    #[allow(clippy::struct_field_names)]
    pub match_info: Option<Match>,
}
