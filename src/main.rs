use core::panic;
use std::collections::HashMap;
use std::collections::HashSet;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::models::MatchInfo;
use anyhow::Result;
use anyhow::{anyhow, Context, Error, Ok}; // Added Context
use chrono::DateTime;
use chrono::Timelike;
use chrono::{Datelike, TimeZone};
use dotenvy::dotenv;
use futures::future::FutureExt;
use futures::select;
use futures::stream;
use futures_util::StreamExt;
use http::HeaderValue;
#[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;
use models::Score;
use riven::consts::Team;
use riven::consts::{Queue, RegionalRoute};
use riven::models::match_v5::Match;
use riven::RiotApi;
use serenity::all::CreateInteractionResponse;
use serenity::all::CreateInteractionResponseMessage;
use serenity::all::CreateMessage;
use serenity::all::GuildId;
use serenity::async_trait;
use serenity::model::prelude::Member;
use serenity::model::{id::ChannelId, prelude::Interaction};
use serenity::prelude::*;
use tokio::sync::RwLock; // Added RwLock explicitly
use tokio::task;
use tokio::time;
use tracing::{error, info};
use tracing_subscriber::{filter, prelude::*, Layer};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

mod commands;
mod models;

// Include the `items` module, which is generated from items.proto.
pub mod log_models {
    include!(concat!(env!("OUT_DIR"), "/log_models.rs"));
    include!(concat!(env!("OUT_DIR"), "/log_models.serde.rs"));
}

#[derive(Debug, Clone)]
struct Config {
    riot_api_token: String,
    discord_bot_token: String,
    voice_channel_id: ChannelId,
    updates_channel_id: ChannelId,
    discord_guild_id: GuildId,
    log_path: PathBuf,
    db_path: PathBuf,
}

fn load_config() -> Result<Config> {
    dotenv().ok();

    let riot_api_token = env::var("RIOT_API_TOKEN").context("Missing RIOT_API_TOKEN")?;
    let discord_bot_token = env::var("DISCORD_BOT_TOKEN").context("Missing DISCORD_BOT_TOKEN")?;

    let voice_channel_id_u64 = env::var("VOICE_CHANNEL_ID")
        .context("Missing VOICE_CHANNEL_ID")?
        .parse::<u64>()
        .context("Invalid VOICE_CHANNEL_ID (must be u64)")?;
    let voice_channel_id = ChannelId::from(voice_channel_id_u64);

    let updates_channel_id_u64 = env::var("UPDATES_CHANNEL_ID")
        .context("Missing UPDATES_CHANNEL_ID")?
        .parse::<u64>()
        .context("Invalid UPDATES_CHANNEL_ID (must be u64)")?;
    let updates_channel_id = ChannelId::from(updates_channel_id_u64);

    let discord_guild_id_u64 = env::var("DISCORD_GUILD_ID")
        .context("Missing DISCORD_GUILD_ID")?
        .parse::<u64>()
        .context("Invalid DISCORD_GUILD_ID (must be u64)")?;
    let discord_guild_id = GuildId::from(discord_guild_id_u64);

    let log_path_str = env::var("LOG_PATH").unwrap_or_else(|_| {
        if cfg!(target_os = "linux") {
            "/var/logs/discord"
        } else {
            "."
        }
        .to_string()
    });
    let log_path = PathBuf::from(log_path_str);

    let db_path_str = env::var("DB_PATH").unwrap_or_else(|_| "sqlite.db".to_string());
    let db_path = PathBuf::from(db_path_str);

    Ok(Config {
        riot_api_token,
        discord_bot_token,
        voice_channel_id,
        updates_channel_id,
        discord_guild_id,
        log_path,
        db_path,
    })
}

macro_rules! riot_api {
    ($response:expr) => {{
        let result = $response;
        match result {
            Result::Err(mut e) => {
                let status_code = { e.status_code().clone() };
                return match status_code {
                    None => Result::Err(anyhow!(
                        "{}",
                        match e.take_response() {
                            Some(r) => {
                                let message = r.text();
                                let text = message.await?;
                                text.clone()
                            }
                            None => String::from("<no response>"),
                        }
                    )),
                    Some(http::status::StatusCode::FORBIDDEN) => panic!("The Riot Key is bad."),
                    Some(http::status::StatusCode::TOO_MANY_REQUESTS) => match e.take_response() {
                        Some(r) => {
                            let wait_time = Duration::from_secs(
                                (String::from_utf8(
                                    r.headers()
                                        .get("retry-after")
                                        .unwrap_or(&HeaderValue::from_str("2").unwrap())
                                        .as_bytes()
                                        .to_vec(),
                                )
                                .unwrap())
                                .parse::<u64>()
                                .unwrap(),
                            ) + Duration::from_millis(
                                (chrono::Local::now().timestamp_millis() % 1000 + 1000) as u64,
                            );
                            tokio::time::sleep(wait_time).await;
                            return Result::Err(anyhow!(
                                "Rate limited - had to wait {} seconds",
                                wait_time.as_secs()
                            ));
                        }
                        None => {
                            tokio::time::sleep(Duration::from_secs(2)).await;
                            return Result::Err(anyhow!(
                                "Rate limited - didn't know what to wait, so waited 2 seconds"
                            ));
                        }
                    },
                    _s => {
                        return Result::Err(anyhow!(e));
                    }
                };
            }
            Result::Ok(s) => Result::<_, anyhow::Error>::Ok(s),
        }
    }};
}

struct Handler {
    users: Arc<RwLock<HashMap<u64, bool>>>,
    current_status: Arc<RwLock<String>>,
    discord_auth: Arc<serenity::http::Http>,
    config: Arc<Config>, // Add config here
}

impl Handler {
    async fn remove_user(&self, member: &Member) {
        let mut users = self.users.write().await;
        users.remove(&member.user.id.get());
        if users.len() == 0 {
            let message = self.current_status.read().await.to_string();
            info!("Len is 0, going to update the status to {}", message);
            match update_discord_status(
                &message,
                None,
                &self.discord_auth,
                self.config.voice_channel_id,
            )
            .await
            {
                Result::Ok(()) => {}
                Err(e) => error!("Error while updating: {}", e),
            }
        } else {
            info!("Users len is {}, not updating.", users.len());
        }
    }
}

fn mode_score(queue_id: &Queue, score: &Score) -> String {
    if *queue_id == Queue::ARENA_2V2V2V2_CHERRY {
        return format!(
            "{}\u{2006}-\u{2006}{}\u{2006}-\u{2006}{}",
            score.wins, score.top_finishes, score.bottom_finishes,
        );
    } else if score.warmup > 0 && score.games == 0 && *queue_id != Queue::SUMMONERS_RIFT_CLASH {
        return format!("🏃");
    }
    format!(
        "{}\u{2006}-\u{2006}{}",
        score.wins,
        score.games - score.wins
    )
}

#[async_trait]
impl EventHandler for Handler {
    async fn ready(&self, ctx: serenity::prelude::Context, _data: serenity::model::prelude::Ready) {
        info!("Ready event received");
        if let Result::Ok(channel) = ctx.http.get_channel(self.config.voice_channel_id).await {
            info!("Games channel passed");
            if let serenity::model::prelude::Channel::Guild(guild_channel) = channel {
                info!("It's a guild channel");
                if let Result::Ok(members) = guild_channel.members(ctx.cache) {
                    info!("We found members");
                    let mut users = self.users.write().await;
                    for member in members {
                        users.insert(member.user.id.get(), false);
                    }
                }
                info!(
                    "Users was updated to have a length of {}",
                    self.users.read().await.len()
                );
            }
        }

        match serenity::model::id::GuildId::set_commands(
            self.config.discord_guild_id,
            &ctx.http,
            vec![
                commands::groups::register(),
                commands::globetrotters::register(),
                commands::register::register(),
            ],
        )
        .await
        {
            Result::Ok(_) => {}
            Result::Err(e) => error!("Ran into error while trying to set up commands: {}", e),
        };
    }

    async fn interaction_create(&self, ctx: serenity::prelude::Context, interaction: Interaction) {
        if let Interaction::Command(command) = interaction {
            info!(
                "Received command interaction: {}",
                command.data.name.as_str()
            );

            let content = match command.data.name.as_str() {
                "groups" => {
                    commands::groups::run(&ctx, &command.data.options, self.config.clone()).await
                }
                "globetrotters" => commands::globetrotters::run(&ctx, &command.data.options),
                "register" => {
                    commands::register::run(&ctx, &command.data.options, self.config.clone()).await
                }
                _ => "not implemented :(".to_string(),
            };

            if command.data.name.as_str() == "register" {
                info!("Register response: {}", content);
                return;
            }

            if let Err(why) = command
                .create_response(
                    &ctx.http,
                    CreateInteractionResponse::Message(
                        CreateInteractionResponseMessage::new().content(content.clone()),
                    ),
                )
                .await
            {
                info!("Cannot respond to slash command: {}", why);
            }
        }
    }

    async fn voice_state_update(
        &self,
        _ctx: serenity::prelude::Context,
        old: Option<serenity::model::voice::VoiceState>,
        new: serenity::model::voice::VoiceState,
    ) {
        // Use config for channel ID
        let voice_channel_id = self.config.voice_channel_id;
        info!(
            "Pattern we're checking is: ({}, {}, {})",
            if old.is_some() { "Some" } else { "None" },
            if new.channel_id.is_some() {
                "Some"
            } else {
                "None"
            },
            if new.member.is_some() { "Some" } else { "None" }
        );
        match (old, new.channel_id, new.member) {
            (None, None, None | Some(_)) => {
                info!("I didn't think the None, None, None | Some(_) pattern could be reached");
            }
            (None, Some(_), None) => {
                info!("I didn't think the None, Some(_), None pattern could be reached");
            }
            (None, Some(new_channel_id), Some(ref member)) => {
                if new_channel_id == voice_channel_id {
                    let mut users = self.users.write().await;
                    users.insert(member.user.id.get(), false);
                }
            }
            (Some(old_channel), None | Some(_), None) => {
                if let Some(channel_id) = old_channel.channel_id {
                    if voice_channel_id == channel_id {
                        if let Some(member) = old_channel.member {
                            self.remove_user(&member).await;
                        }
                    }
                }
            }
            (Some(old_channel), None, Some(member)) => {
                if let Some(channel_id) = old_channel.channel_id {
                    if voice_channel_id == channel_id {
                        if let Some(old_member) = old_channel.member {
                            if old_member.user.id == member.user.id {
                                self.remove_user(&member).await;
                            }
                        }
                    }
                }
            }
            (Some(old_channel), Some(new_channel_id), Some(ref member)) => {
                if new_channel_id == voice_channel_id {
                    let mut users = self.users.write().await;
                    users.insert(member.user.id.get(), false);
                } else if let Some(channel_id) = old_channel.channel_id {
                    if voice_channel_id == channel_id {
                        if let Some(old_member) = old_channel.member {
                            if old_member.user.id == member.user.id {
                                self.remove_user(member).await;
                            }
                        }
                    }
                }
            }
        }
        info!(
            "After checking, the count is now {}",
            (self.users.read().await).len()
        );
    }
}

fn get_start_time() -> Result<i64> {
    let mut datetime = chrono::Local::now();
    if datetime.time().hour() < 8 {
        datetime = match datetime.checked_sub_days(chrono::naive::Days::new(1)) {
            Some(d) => d,
            None => {
                // Use context for better error message
                return Err(anyhow!("Unable to subtract day from current time"));
            }
        }
    }
    // Use context for better error message if unwrap fails (though unlikely here)
    datetime = chrono::Local
        .with_ymd_and_hms(
            datetime.date_naive().year(),
            datetime.date_naive().month(),
            datetime.date_naive().day(),
            6,
            0,
            0,
        )
        .single() // Handle ambiguity (e.g., DST change)
        .context("Failed to construct start time")?;
    Result::Ok(datetime.timestamp())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load config first
    let config = load_config().context("Failed to load configuration")?;
    let config_arc = Arc::new(config.clone()); // Clone for Arc

    std::panic::set_hook(Box::new(|i| {
        error!("Panic'd: {}", i);
    }));

    // Use config for Riot API key
    let riot_api = RiotApi::new(&config.riot_api_token);

    // Use config for log path
    let file_appender = tracing_appender::rolling::daily(&config.log_path, "server.log");
    let (non_blocking_appender, _guard) = tracing_appender::non_blocking(file_appender);
    let console_layer = console_subscriber::ConsoleLayer::builder()
        .server_addr(([0, 0, 0, 0], 5555))
        .spawn();

    tracing_subscriber::registry()
        .with(console_layer)
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(non_blocking_appender)
                .with_filter(filter::filter_fn(|metadata| {
                    metadata.target().starts_with("discord_status_bot")
                })),
        )
        .init();

    let active_users: Arc<RwLock<HashMap<u64, bool>>> = Arc::new(RwLock::new(HashMap::new()));
    let current_status: Arc<RwLock<String>> = Arc::new(RwLock::new(String::new()));

    // Use config for Discord bot token
    let intents = GatewayIntents::non_privileged()
        | GatewayIntents::GUILDS
        | GatewayIntents::GUILD_MEMBERS
        | GatewayIntents::GUILD_MESSAGES;

    let http_client = reqwest::Client::builder()
        .use_rustls_tls()
        .connection_verbose(true)
        .build()?;
    let discord_client = Arc::new(
        serenity::http::HttpBuilder::new(&config.discord_bot_token)
            .ratelimiter_disabled(true)
            .client(http_client)
            .build(),
    );

    let handler = Handler {
        users: active_users.clone(), // Keep cloning Arcs
        current_status: current_status.clone(),
        discord_auth: discord_client.clone(),
        config: config_arc.clone(),
    };

    let mut client = Client::builder(&config.discord_bot_token, intents)
        .event_handler(handler)
        .await
        .context("Error creating Discord client")?;

    let discord_events = tokio::spawn(async move {
        client
            .start()
            .await
            .map_err(|e| anyhow!("Discord client error: {}", e)) // Convert Serenity error
    });

    // --- Background Task ---
    // Clone necessary Arcs and config values for the background task
    let background_config = config_arc.clone();
    let background_riot_api = Arc::new(riot_api); // Put RiotApi in Arc if needed across awaits
    let background_discord_client = discord_client.clone();
    let background_current_status = current_status.clone();
    // let background_active_users = active_users.clone(); // Not currently used in loop, but if needed

    let forever: task::JoinHandle<Result<()>> = task::spawn(async move {
        let mut start_time = get_start_time().unwrap_or_else(|e| {
            error!(
                "Failed to get initial start time: {}. Using current time.",
                e
            );
            chrono::Local::now().timestamp()
        });
        let mut current_date = start_time; // Initialize current_date based on initial start_time logic
        let mut interval = time::interval(Duration::from_secs(5)); // Consider making interval configurable

        // Use config for DB path
        let db_path = &background_config.db_path;
        let summoners = fetch_summoners(db_path)?;
        let active_summoner_names = summoners.values().cloned().collect::<HashSet<_>>(); // Collect names for puuid fetch

        loop {
            // Fetch PUUIDs based on currently known summoners
            // Note: This doesn't automatically pick up newly registered summoners until restart
            // Consider refetching summoners periodically or triggering an update on registration
            let puuids = fetch_puuids(db_path, &active_summoner_names)?;

            // Check for new day *before* iterating PUUIDs
            let new_start_time = get_start_time()?;
            let is_new_day = current_date < new_start_time;

            if is_new_day {
                info!("It's a new day, time to scan histories at least once. Current day was {}, now it's {}", current_date, new_start_time);
                match check_match_history(
                    &background_riot_api,
                    &background_discord_client,
                    &background_current_status,
                    &background_config,
                )
                .await
                {
                    Result::Ok(()) => {}
                    Result::Err(e) => error!("Error during daily history check: {}", e),
                }
                start_time = chrono::Local::now().timestamp(); // Reset scan window start
                info!(
                    "Daily check done. Setting scan start time to {}",
                    start_time
                );
                current_date = new_start_time; // Update the date marker
            }

            let mut found_new_matches = false;
            for puuid in &puuids {
                interval.tick().await; // Rate limit per PUUID check
                let matches_fetch: Result<Vec<String>> = riot_api!(
                    background_riot_api // Use the API client from the closure
                        .match_v5()
                        .get_match_ids_by_puuid(
                            RegionalRoute::AMERICAS,
                            puuid,
                            None,
                            None,
                            None,
                            Some(start_time), // Check only since last scan OR daily reset
                            None,
                            None,
                        )
                        .await
                );

                match matches_fetch {
                    Result::Ok(match_list) if !match_list.is_empty() => {
                        info!(
                            "Found {} new match(es) for a user since timestamp {}. Triggering full scan.",
                            match_list.len(),
                            start_time
                        );
                        found_new_matches = true;
                        break; // Found matches for one user, no need to check others, proceed to full scan
                    }
                    Result::Ok(_) => { /* No new matches for this user */ }
                    Result::Err(e) => {
                        error!("Hit an error while fetching matches for {}: {}", puuid, e)
                    }
                }
            }

            // If new matches were found for *any* user, run the full history check
            if found_new_matches {
                match check_match_history(
                    &background_riot_api,
                    &background_discord_client,
                    &background_current_status,
                    &background_config,
                )
                .await
                {
                    Result::Ok(()) => {}
                    Result::Err(e) => error!("Error during triggered history check: {}", e),
                }
                start_time = chrono::Local::now().timestamp(); // Reset scan window start after processing
                info!(
                    "Triggered check done. Setting scan start time to {}",
                    start_time
                );
            }

            // If it wasn't a new day and no new matches were found, just wait for the next interval cycle
            if !is_new_day && !found_new_matches {
                interval.tick().await; // Ensure we wait at least the interval duration even if loop was fast
            }
        }
        // This loop runs forever, so Ok(()) might not be reached unless there's a panic/error
        // Ok(())
    });

    // --- Main Select Loop ---
    match select! {
        // Make sure results are handled correctly
        a_res = discord_events.fuse() => {
            error!("Discord client task finished unexpectedly.");
            a_res? // Propagate potential JoinError
        },
        b_res = forever.fuse() => {
            error!("Background check task finished unexpectedly.");
            b_res? // Propagate potential JoinError
        }
    } {
        Result::Ok(res) => {
            // This block might not be reachable if tasks loop forever
            info!("A task finished successfully? {:?}", res);
            Ok(res) // Return the inner Result<()>
        }
        Result::Err(e) => {
            error!("A critical task failed: {:?}", e);
            Err(e) // Propagate the anyhow::Error
        }
    }
}

// --- Database Functions (Accept db_path) ---

fn update_match(
    db_path: &PathBuf,
    match_id: &String,
    queue_id: i64,
    is_win: bool,
    end_timestamp: i64,
    match_info: &Match,
) -> Result<()> {
    let connection = rusqlite::Connection::open(db_path)?; // Use db_path
    let mut statement = connection
        .prepare("INSERT OR IGNORE INTO match (id, queue_id, win, end_timestamp, MatchInfo) VALUES (?1, ?2, ?3, ?4, ?5);")?;
    statement.execute(rusqlite::params![
        // Use params! macro for clarity
        match_id,
        queue_id,
        is_win, // rusqlite handles bool conversion
        end_timestamp,
        &serde_json::to_string(match_info)?,
    ])?;
    Ok(())
}

fn fetch_discord_usernames(db_path: &PathBuf) -> Result<HashMap<String, String>> {
    let mut summoner_map: HashMap<String, String> = HashMap::new();
    let connection = rusqlite::Connection::open(db_path)?; // Use db_path
    let mut statement = connection.prepare("SELECT DiscordId, LOWER(summonerName) FROM User")?;
    let mut rows = statement.query([])?; // Use query instead of query_map for simpler iteration

    while let Some(row) = rows.next()? {
        let discord_id: String = row.get(0)?; // Use ? for error handling
        let summoner: String = row.get(1)?;
        summoner_map.insert(summoner, discord_id);
    }
    Ok(summoner_map)
}

fn fetch_puuids(db_path: &PathBuf, summoner_names: &HashSet<String>) -> Result<HashSet<String>> {
    if summoner_names.is_empty() {
        return Ok(HashSet::new());
    }
    let connection = rusqlite::Connection::open(db_path)?; // Use db_path

    // Use parameterized query
    let placeholders: String = summoner_names
        .iter()
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT puuid FROM summoner WHERE name IN ({})",
        placeholders
    );
    let mut statement = connection.prepare(&sql)?;

    // Convert HashSet<String> to Vec<&str> for binding
    let params_vec: Vec<&str> = summoner_names.iter().map(|s| s.as_str()).collect();

    let puuids = statement
        .query_map(rusqlite::params_from_iter(params_vec), |row| row.get(0))?
        .collect::<Result<HashSet<String>, _>>()?; // Collect results directly into HashSet

    Ok(puuids)
}

fn fetch_summoners(db_path: &PathBuf) -> Result<HashMap<u64, String>> {
    let connection = rusqlite::Connection::open(db_path)?; // Use db_path
    let mut statement = connection.prepare("SELECT DiscordId, SummonerName FROM USER;")?;
    let mut summoners = HashMap::new();
    let mut rows = statement.query([])?;

    while let Some(row) = rows.next()? {
        let discord_id_str: String = row.get(0)?;
        let summoner_name: String = row.get(1)?;
        let discord_id = discord_id_str
            .parse::<u64>()
            .with_context(|| format!("Failed to parse DiscordId '{}' as u64", discord_id_str))?;
        summoners.insert(discord_id, summoner_name);
    }
    Ok(summoners)
}

async fn fetch_seen_events(
    db_path: &PathBuf,
    match_ids: &Vec<String>,
) -> Result<HashMap<String, MatchInfo>> {
    if match_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let connection = rusqlite::Connection::open(db_path)?; // Use db_path

    // Use parameterized query
    let placeholders = match_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let sql = format!(
        "SELECT id, queue_id, win, IFNULL(end_timestamp, 0), MatchInfo FROM match WHERE id IN ({})",
        placeholders
    );
    let mut statement = connection.prepare(&sql)?;
    let params = rusqlite::params_from_iter(match_ids);

    let mut seen_matches: HashMap<String, MatchInfo> = HashMap::new();
    let rows = statement.query_map(params, |row| {
        let id: String = row.get(0)?;
        let queue_id: i64 = row.get(1)?;
        let win: bool = row.get(2)?; // Read bool directly
        let timestamp_ms: i64 = row.get(3)?;
        let match_info_str: Option<String> = row.get(4)?; // Handle potentially NULL MatchInfo

        let end_timestamp = DateTime::from_timestamp_millis(timestamp_ms)
            .ok_or_else(|| rusqlite::Error::InvalidQuery)?; // Handle potential timestamp error

        // Handle potentially null or invalid JSON
        let match_info: Option<Match> = match match_info_str {
            Some(s) if !s.is_empty() => serde_json::from_str(&s).map_err(|e| {
                error!("Failed to parse MatchInfo JSON for match {}: {}", id, e);
                rusqlite::Error::FromSqlConversionFailure(
                    4,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?,
            _ => None,
        };

        Result::Ok(MatchInfo {
            id,
            queue_id: queue_id.to_string(),
            win,
            end_timestamp,
            match_info,
        })
    })?;

    for result in rows {
        match result {
            std::result::Result::Ok(match_info) => {
                seen_matches.insert(match_info.id.clone(), match_info);
            }
            Err(e) => {
                // Log error but continue processing other rows if possible
                error!("Error processing row in fetch_seen_events: {}", e);
            }
        }
    }
    Ok(seen_matches)
}

// --- Core Logic Functions (Accept config or relevant parts) ---

async fn check_match_history(
    riot_api: &RiotApi,
    discord_client: &Arc<serenity::http::Http>,
    current_status: &Arc<RwLock<String>>,
    config: &Config,
) -> Result<()> {
    let queue_ids: HashMap<Queue, &str> = HashMap::from([
        (Queue::SUMMONERS_RIFT_5V5_DRAFT_PICK, "Draft"),
        (Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO, "Solo"),
        (Queue::SUMMONERS_RIFT_5V5_BLIND_PICK, "Blind"),
        (Queue::SUMMONERS_RIFT_5V5_RANKED_FLEX, "Flex"),
        (Queue::SUMMONERS_RIFT_NORMAL_SWIFTPLAY, "Swiftplay"),
        (Queue::HOWLING_ABYSS_5V5_ARAM, "ARAM"),
        (Queue::SUMMONERS_RIFT_CLASH, "Clash"),
        (Queue::ARENA_2V2V2V2_CHERRY, "Arena"),
    ]);
    let mut queue_scores: HashMap<Queue, Score> = HashMap::new();
    let start_time = get_start_time()?;
    let db_path = &config.db_path; // Get DB path from config

    let summoner_info = fetch_summoners(db_path)?;
    let summoner_names = summoner_info.values().cloned().collect::<HashSet<_>>();
    let puuids = fetch_puuids(db_path, &summoner_names)?;

    // Use a simple Vec to collect all match IDs, then deduplicate
    let mut all_match_ids: Vec<String> = Vec::new();
    let matches_requests = stream::iter(puuids.clone())
        .map(|puuid| async move {
            riot_api!(
                riot_api
                    .match_v5()
                    .get_match_ids_by_puuid(
                        RegionalRoute::AMERICAS,
                        &puuid,
                        None,
                        None,
                        None,
                        Some(start_time),
                        None,
                        None,
                    )
                    .await
            )
        })
        .buffer_unordered(5); // Consider making buffer size configurable

    let matches_list: Vec<Result<Vec<String>>> = matches_requests.collect().await;

    for result in matches_list {
        match result {
            Result::Ok(match_ids) => {
                all_match_ids.extend(match_ids);
            }
            Result::Err(e) => {
                // Log error but continue processing other PUUIDs
                error!("Error fetching match IDs for a PUUID: {:?}", e);
            }
        }
    }

    // Deduplicate and sort match IDs
    let mut unique_match_ids: Vec<String> = all_match_ids
        .into_iter()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    unique_match_ids.sort(); // Sort for consistent processing order

    if unique_match_ids.is_empty() {
        info!("No relevant matches found since start time {}", start_time);
        // Update status to "No games" if it wasn't already, maybe?
        // Or just return early if no matches need processing.
        // Let's update the status to ensure it reflects reality if empty.
        let results = String::from("No LoL games yet!");
        update_discord_status(
            &results,
            Some(current_status),
            discord_client,
            config.voice_channel_id,
        )
        .await?;
        return Ok(());
    }

    // Fetch info only for matches we haven't processed yet
    let seen_matches = fetch_seen_events(db_path, &unique_match_ids).await?;
    let mut pentakillers = Vec::new();
    let mut dom_earners = Vec::new();

    let mut matches_to_fetch_details = Vec::new();
    for match_id in &unique_match_ids {
        if let Some(match_info) = seen_matches.get(match_id) {
            // Process already seen match for score calculation
            process_seen_match_score(match_info, &mut queue_scores, &puuids)?;
        } else {
            // Add to list of matches needing full details
            matches_to_fetch_details.push(match_id.clone());
        }
    }

    // Fetch details for unseen matches concurrently
    let match_detail_requests = stream::iter(matches_to_fetch_details)
        .map(|match_id| async move {
            let result = riot_api!(
                riot_api
                    .match_v5()
                    .get_match(RegionalRoute::AMERICAS, &match_id)
                    .await
            );
            Ok((match_id, result)) // Return ID along with result
        })
        .buffer_unordered(10); // Adjust buffer size as needed

    let match_details: Vec<(String, Result<Option<Match>, Error>)> = match_detail_requests
        .collect::<Vec<Result<(String, Result<Option<Match>, Error>), Error>>>()
        .await
        .into_iter()
        .filter_map(Result::ok)
        .collect();

    for (match_id, result) in match_details {
        match result {
            Result::Ok(Some(match_info)) => {
                // Process the newly fetched match details
                update_match_info(
                    db_path,
                    &match_info,
                    &mut queue_scores,
                    &queue_ids,
                    &puuids,
                    &mut pentakillers,
                    &mut dom_earners,
                    start_time,
                )?;
            }
            Result::Ok(None) => {
                error!("Riot API returned Ok(None) for match id {}", match_id);
            }
            Result::Err(e) => {
                error!("Failed to fetch details for match {}: {:?}", match_id, e);
            }
        }
    }

    check_pentakill_info(pentakillers, discord_client, config).await?;
    check_doms_info(dom_earners, discord_client, config).await?;

    let status_message = generate_status_message(&queue_scores, &queue_ids);

    update_discord_status(
        &status_message,
        Some(current_status),
        discord_client,
        config.voice_channel_id,
    )
    .await
}

fn process_seen_match_score(
    match_info: &MatchInfo,
    queue_scores: &mut HashMap<Queue, Score>,
    puuids: &HashSet<String>,
) -> Result<()> {
    let queue_id;
    if let Some(info) = &match_info.match_info {
        queue_id = info.info.queue_id;
    } else {
        return Ok(());
    }

    let game_score = queue_scores.entry(queue_id).or_insert_with(Score::default);

    let early_surrender = match &match_info.match_info {
        Some(info) => info
            .info
            .participants
            .first()
            .map_or(false, |p| p.game_ended_in_early_surrender),
        None => false,
    };

    if early_surrender {
        return Ok(());
    }

    if queue_id == Queue::ARENA_2V2V2V2_CHERRY {
        game_score.games += 1;
        if let Some(info) = &match_info.match_info {
            let mut seen_placements = HashSet::new();
            for participant in info.info.participants.iter() {
                if puuids.contains(&participant.puuid) {
                    if let Some(position) = participant.placement {
                        if seen_placements.insert(position) {
                            if position == 1 {
                                game_score.wins += 1;
                            }
                            if position <= 4 {
                                game_score.top_finishes += 1;
                            } else {
                                game_score.bottom_finishes += 1;
                            }
                        }
                    }
                }
            }
        } else {
            error!(
                "Cannot calculate Arena score for seen match {} without details.",
                match_info.id
            );
        }
    } else {
        game_score.games += 1;
        if match_info.win {
            game_score.wins += 1;
        } else if game_score.warmup < 2
            && game_score.wins == 0
            && queue_id != Queue::SUMMONERS_RIFT_CLASH
        {
            game_score.warmup += 1;
            game_score.games -= 1;
        }
    }

    Ok(())
}

fn generate_status_message(
    queue_scores: &HashMap<Queue, Score>,
    queue_ids: &HashMap<Queue, &str>,
) -> String {
    if queue_scores.is_empty() || queue_scores.values().all(|s| s.games == 0 && s.warmup == 0) {
        return String::from("No LoL games yet!");
    }

    let mut queue_scores_msgs: Vec<_> = queue_scores
        .iter()
        .filter(|(_, score)| score.games > 0 || score.warmup > 0)
        .filter_map(|(queue_id, score)| {
            queue_ids.get(queue_id).map(|&game_mode| {
                (
                    game_mode,
                    format!("{}:\u{2006}{}", game_mode, mode_score(queue_id, score)),
                )
            })
        })
        .collect();
    queue_scores_msgs.sort_by(|a, b| a.0.cmp(b.0)); // Sort by game mode name

    let mut results = queue_scores_msgs
        .iter()
        .map(|(_, msg)| msg.clone())
        .collect::<Vec<_>>()
        .join("\u{2006}|\u{2006}");

    const MAX_STATUS_LEN: usize = 40; // Define max length as constant

    if results.chars().count() <= MAX_STATUS_LEN {
        return results;
    }

    queue_scores_msgs = queue_scores
        .iter()
        .filter(|(_, score)| score.games > 0 || score.warmup > 0)
        .filter_map(|(queue_id, score)| {
            queue_ids.get(queue_id).map(|&game_mode| {
                let short_mode = if game_mode == "Swiftplay" {
                    "Swift"
                } else {
                    game_mode
                };
                (
                    game_mode,
                    format!("{}:\u{2006}{}", short_mode, mode_score(queue_id, score)),
                )
            })
        })
        .collect();
    queue_scores_msgs.sort_by(|a, b| a.0.cmp(b.0));
    results = queue_scores_msgs
        .iter()
        .map(|(_, msg)| msg.clone())
        .collect::<Vec<_>>()
        .join("\u{2006}|\u{2006}");

    if results.chars().count() <= MAX_STATUS_LEN {
        return results;
    }

    queue_scores_msgs = queue_scores
        .iter()
        .filter(|(_, score)| score.games > 0 || score.warmup > 0)
        .filter_map(|(queue_id, score)| {
            queue_ids.get(queue_id).and_then(|&game_mode| {
                game_mode.chars().next().map(|initial| {
                    let short_mode = initial.to_uppercase().to_string();
                    (
                        game_mode,
                        format!("{}:\u{2006}{}", short_mode, mode_score(queue_id, score)),
                    )
                })
            })
        })
        .collect();
    queue_scores_msgs.sort_by(|a, b| a.0.cmp(b.0));
    results = queue_scores_msgs
        .iter()
        .map(|(_, msg)| msg.clone())
        .collect::<Vec<_>>()
        .join("\u{2006}|\u{2006}");

    results
}

fn update_match_info(
    db_path: &PathBuf,
    match_info: &Match,
    queue_scores: &mut HashMap<Queue, Score>,
    queue_ids: &HashMap<Queue, &str>,
    puuids: &HashSet<String>,
    pentakillers: &mut Vec<(i64, String)>,
    dom_earners: &mut Vec<(i64, String)>,
    start_time: i64,
) -> Result<()> {
    let Some(game_end_timestamp) = match_info.info.game_end_timestamp else {
        info!(
            "Match {} has no game_end_timestamp. Skipping.",
            match_info.metadata.match_id
        );
        return Ok(());
    };

    if game_end_timestamp < start_time {
        info!(
            "Match {} ended before start_time {}. Skipping.",
            match_info.metadata.match_id, start_time
        );
        return Ok(());
    }

    if !queue_ids.contains_key(&match_info.info.queue_id) {
        info!(
            "Match {} has untracked queue ID {}. Skipping.",
            match_info.metadata.match_id, match_info.info.queue_id
        );
        return Ok(());
    }

    let game_score = queue_scores
        .entry(match_info.info.queue_id)
        .or_insert_with(Score::default);

    let early_surrender = match_info
        .info
        .participants
        .first()
        .map_or(false, |p| p.game_ended_in_early_surrender);

    if early_surrender {
        info!(
            "Match {} ended in early surrender. Skipping score update.",
            match_info.metadata.match_id
        );
        let queue_id: i64 = u16::from(match_info.info.queue_id).into();
        update_match(
            db_path,
            &match_info.metadata.match_id,
            queue_id,
            false,
            game_end_timestamp,
            match_info,
        )?;
        return Ok(());
    }

    let our_team_id = match match_info
        .info
        .participants
        .iter()
        .find(|p| puuids.contains(&p.puuid))
    {
        Some(p) => p.team_id,
        None => {
            error!(
                "Could not find any tracked PUUIDs in match {}. Skipping score update.",
                match_info.metadata.match_id
            );
            return Ok(());
        }
    };

    let queue_id: i64 = u16::from(match_info.info.queue_id).into();
    let mut is_win = false;

    if match_info.info.queue_id == Queue::ARENA_2V2V2V2_CHERRY {
        game_score.games += 1;
        let mut seen_placements = HashSet::new();
        for participant in match_info.info.participants.iter() {
            if puuids.contains(&participant.puuid) {
                if let Some(position) = participant.placement {
                    if seen_placements.insert(position) {
                        if position == 1 {
                            is_win = true;
                            game_score.wins += 1;
                        }
                        if position <= 4 {
                            game_score.top_finishes += 1;
                        } else {
                            game_score.bottom_finishes += 1;
                        }
                    }
                }
            }
        }
    } else {
        game_score.games += 1;
        if let Some(team) = match_info
            .info
            .teams
            .iter()
            .find(|t| t.team_id == our_team_id)
        {
            if team.win {
                is_win = true;
                game_score.wins += 1;
            } else if game_score.warmup < 2
                && game_score.wins == 0 // Only count warmup if no wins yet in this mode
                && match_info.info.queue_id != Queue::SUMMONERS_RIFT_CLASH
            {
                game_score.warmup += 1;
                game_score.games -= 1; // Don't count warmup towards total games played
            }
        } else {
            error!(
                "Could not find team data for team ID {} in match {}.",
                match our_team_id {
                    Team::BLUE => "Blue",
                    Team::RED => "Red",
                    _ => "Other",
                },
                match_info.metadata.match_id
            );
        }
    }

    update_match(
        db_path,
        &match_info.metadata.match_id,
        queue_id,
        is_win,
        game_end_timestamp,
        match_info,
    )?;

    dom_earners.extend(
        match_info
            .info
            .participants
            .iter()
            .filter(|p| puuids.contains(&p.puuid)) // Use borrowed puuids
            .filter(|p| p.kills == 5 && p.assists == 5 && p.deaths == 5)
            .map(|p| (match_info.info.game_id, p.summoner_name.clone())),
    );

    if match_info.info.queue_id != Queue::HOWLING_ABYSS_5V5_ARAM {
        pentakillers.extend(
            match_info
                .info
                .participants
                .iter()
                .filter(|p| puuids.contains(&p.puuid)) // Use borrowed puuids
                .filter(|p| p.penta_kills > 0)
                .map(|p| (match_info.info.game_id, p.summoner_name.clone())),
        );
    }
    Ok(())
}

// Helper function to format names list
fn format_names_list(names: &[String]) -> String {
    match names.len() {
        0 => String::new(),
        1 => names[0].clone(),
        2 => format!("{} and {}", names[0], names[1]),
        _ => format!(
            "{}, and {}",
            names[..names.len() - 1].join(", "),
            names.last().unwrap()
        ),
    }
}

async fn check_pentakill_info(
    pentakillers: Vec<(i64, String)>,
    discord_auth: &Arc<serenity::http::Http>,
    config: &Config,
) -> Result<()> {
    if pentakillers.is_empty() {
        return Ok(());
    }

    let user_info = fetch_discord_usernames(&config.db_path)?;
    let mut pentakillers_by_discord_mention: Vec<_> = pentakillers
        .iter()
        .filter_map(|(_, summoner_name)| {
            user_info
                .get(&summoner_name.to_lowercase())
                .map(|discord_id| format!("<@{}>", discord_id))
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    pentakillers_by_discord_mention.sort();

    let mut pentakill_games: HashMap<i64, Vec<String>> = HashMap::new();
    for (game_id, name) in pentakillers {
        pentakill_games.entry(game_id).or_default().push(name);
    }

    let mention_names_formatted = format_names_list(&pentakillers_by_discord_mention);

    let mut pentakill_links = Vec::new();
    for (game_id, names) in pentakill_games {
        let game_names_formatted = format_names_list(&names);
        let alternative = String::from("player");
        let url_encoded_name = urlencoding::encode(names.first().unwrap_or(&alternative));
        pentakill_links.push(format!(
            "[{}'s pentakill game here](https://blitz.gg/lol/match/na1/{}/{})",
            game_names_formatted, url_encoded_name, game_id
        ));
    }

    update_highlight_reel(
        &format!(
            "Make sure to congratulate {} on their pentakill{}!\n\n{}",
            mention_names_formatted,
            if pentakillers_by_discord_mention.len() > 1 {
                "s"
            } else {
                ""
            },
            pentakill_links.join("\n")
        ),
        discord_auth,
        config.updates_channel_id,
    )
    .await?;

    Ok(())
}

async fn check_doms_info(
    dom_earners: Vec<(i64, String)>,
    discord_auth: &Arc<serenity::http::Http>,
    config: &Config,
) -> Result<()> {
    if dom_earners.is_empty() {
        return Ok(());
    }

    let user_info = fetch_discord_usernames(&config.db_path)?;
    let mut dom_earners_by_discord_mention: Vec<_> = dom_earners
        .iter()
        .filter_map(|(_, summoner_name)| {
            user_info
                .get(&summoner_name.to_lowercase())
                .map(|discord_id| format!("<@{}>", discord_id))
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    dom_earners_by_discord_mention.sort();

    let mut dom_games: HashMap<i64, Vec<String>> = HashMap::new();
    for (game_id, name) in dom_earners {
        dom_games.entry(game_id).or_default().push(name);
    }

    let mention_names_formatted = format_names_list(&dom_earners_by_discord_mention);

    let mut dom_links = Vec::new();
    for (game_id, names) in dom_games {
        let game_names_formatted = format_names_list(&names);
        let alternative = String::from("player");
        let url_encoded_name = urlencoding::encode(names.first().unwrap_or(&alternative));
        dom_links.push(format!(
            "[{}'s dom game here](https://blitz.gg/lol/match/na1/{}/{})",
            game_names_formatted, url_encoded_name, game_id
        ));
    }

    update_highlight_reel(
        "🚨 SOMEONE JUST GOT DOMS 🚨",
        discord_auth,
        config.updates_channel_id,
    )
    .await?;
    update_highlight_reel(
        "https://tenor.com/view/dominos-dominos-pizza-pizza-dominos-guy-gif-7521435",
        discord_auth,
        config.updates_channel_id,
    )
    .await?;
    update_highlight_reel(
        &format!(
            "{} just earned doms! Here's their game{}:\n{}",
            mention_names_formatted,
            if dom_links.len() > 1 { "s" } else { "" },
            dom_links.join("\n")
        ),
        discord_auth,
        config.updates_channel_id,
    )
    .await?;

    Ok(())
}

async fn update_discord_status(
    results: &str,
    current_status: Option<&Arc<RwLock<String>>>,
    discord_auth: &Arc<serenity::http::Http>,
    voice_channel_id: ChannelId,
) -> Result<()> {
    if let Some(status_arc) = current_status {
        let mut s = status_arc.write().await;
        if *s != results {
            *s = results.to_string();
            info!("Current status state updated to '{}'", results);
        } else {
            info!(
                "Discord status already matches '{}'. Skipping update.",
                results
            );
            return Ok(());
        }
    } else {
        info!(
            "Updating Discord status to '{}' (no state tracking)",
            results
        );
    }

    let mut content = serde_json::Map::new();
    content.insert(String::from("status"), results.to_string().into());

    discord_auth
        .edit_voice_status(voice_channel_id, &content, None)
        .await?;
    info!("Successfully sent voice status update to Discord.");

    Ok(())
}

async fn update_highlight_reel(
    results: &str,
    discord_auth: &Arc<serenity::http::Http>,
    updates_channel_id: ChannelId,
) -> Result<()> {
    let message = CreateMessage::new().content(results);
    match discord_auth
        .send_message(updates_channel_id, vec![], &message)
        .await
    {
        Result::Ok(message) => info!("Highlight message sent: {}", message.id),
        Result::Err(e) => error!("Error sending highlight message: {}", e),
    };

    Ok(())
}
