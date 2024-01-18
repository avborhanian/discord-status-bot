use riven::models::match_v5::Match;

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
    #[allow(clippy::struct_field_names)]
    pub match_info: Option<Match>,
}
