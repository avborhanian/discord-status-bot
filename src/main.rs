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
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Row, SqlitePool};
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
    pool: sqlx::SqlitePool,
}

async fn load_config() -> Result<Config> {
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

    let db_path_str = env::var("DB_PATH").unwrap_or_else(|_| "sqlite://sqlite.db".to_string());

    let pool = SqlitePoolOptions::new()
        .max_connections(5) // Configure pool size
        .connect(&db_path_str) // Use database_url from config
        .await
        .context("Failed to create SQLite connection pool")?;

    Ok(Config {
        riot_api_token,
        discord_bot_token,
        voice_channel_id,
        updates_channel_id,
        discord_guild_id,
        log_path,
        pool,
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
                _ => Err(anyhow!("not implemented :(".to_string())),
            };

            if let Err(why) = command
                .create_response(
                    &ctx.http,
                    CreateInteractionResponse::Message(
                        CreateInteractionResponseMessage::new().content(content.unwrap_or_else(
                            |error| {
                                format!(
                                    "An error occurred while processing your command: {}",
                                    error
                                )
                            },
                        )),
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
    let config = load_config()
        .await
        .context("Failed to load configuration")?;
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
        let pool = &background_config.pool;
        let summoners = fetch_summoners(pool).await?;
        let active_summoner_names = summoners.values().cloned().collect::<HashSet<_>>(); // Collect names for puuid fetch

        loop {
            // Fetch PUUIDs based on currently known summoners
            // Note: This doesn't automatically pick up newly registered summoners until restart
            // Consider refetching summoners periodically or triggering an update on registration
            let puuids = fetch_puuids(pool, &active_summoner_names).await?;

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

// --- Database Functions (Accept pool) ---

async fn update_match(
    pool: &sqlx::SqlitePool,
    match_id: &String,
    queue_id: i64,
    is_win: bool,
    end_timestamp: i64,
    match_info: &Match,
) -> Result<()> {
    let match_info_json = serde_json::to_string(match_info)?;

    sqlx::query(
        "INSERT OR IGNORE INTO match (id, queue_id, win, end_timestamp, MatchInfo) VALUES (?, ?, ?, ?, ?);"
    )
    .bind(match_id)
    .bind(queue_id)
    .bind(is_win)
    .bind(end_timestamp)
    .bind(&match_info_json) // Bind JSON string
    .execute(pool) // Execute against the pool
    .await?; // Await the result

    Ok(())
}

async fn fetch_discord_usernames(pool: &SqlitePool) -> Result<HashMap<String, String>> {
    // Fetch rows as tuples (String, String)
    let mappings: Vec<(String, String)> =
        sqlx::query_as("SELECT DiscordId, LOWER(summonerName) FROM User")
            .fetch_all(pool) // Fetch all results into a Vec
            .await?;

    // Convert Vec<(DiscordId, LowerSummonerName)> to HashMap<LowerSummonerName, DiscordId>
    let summoner_map: HashMap<String, String> = mappings
        .into_iter()
        .map(|(id, name)| (name, id)) // Flip order for map
        .collect();

    Ok(summoner_map)
}

async fn fetch_puuids(
    pool: &SqlitePool,
    summoner_names: &HashSet<String>,
) -> Result<HashSet<String>> {
    if summoner_names.is_empty() {
        return Ok(HashSet::new());
    }

    // Convert HashSet to Vec for binding
    let names_vec: Vec<String> = summoner_names.iter().cloned().collect();
    // Serialize the Vec to a JSON string for the IN clause workaround
    let names_json = serde_json::to_string(&names_vec)?;

    // Use query_scalar to fetch a single column directly
    // The `json_each(?)` trick allows binding a list to an IN clause in SQLite with sqlx
    let puuids: Vec<String> = sqlx::query_scalar(
        "SELECT puuid FROM summoner WHERE name IN (SELECT value FROM json_each(?))",
    )
    .bind(names_json) // Bind the JSON array string
    .fetch_all(pool)
    .await?;

    // Collect into HashSet
    Ok(puuids.into_iter().collect())
}

async fn fetch_summoners(pool: &SqlitePool) -> Result<HashMap<u64, String>> {
    // Fetch rows as tuples (String, String)
    let rows: Vec<(String, String)> = sqlx::query_as("SELECT DiscordId, SummonerName FROM USER")
        .fetch_all(pool)
        .await?;

    let mut summoners = HashMap::new();
    for (discord_id_str, summoner_name) in rows {
        // Parse DiscordId String to u64
        let discord_id = discord_id_str
            .parse::<u64>()
            .with_context(|| format!("Failed to parse DiscordId '{}' as u64", discord_id_str))?;
        summoners.insert(discord_id, summoner_name);
    }
    Ok(summoners)
}

async fn fetch_seen_events(
    pool: &SqlitePool, // Accept pool
    match_ids: &Vec<String>,
) -> Result<HashMap<String, MatchInfo>> {
    if match_ids.is_empty() {
        return Ok(HashMap::new());
    }

    // Serialize Vec<String> to JSON for IN clause
    let ids_json = serde_json::to_string(match_ids)?;

    // Fetch all rows matching the IDs
    let rows = sqlx::query( "SELECT Id, QUEUE_ID, Win, IFNULL(END_TIMESTAMP, 0) as end_ts, MatchInfo FROM MATCH WHERE Id IN (SELECT value FROM json_each(?))").bind(ids_json).fetch_all(pool).await?;

    let mut seen_matches: HashMap<String, MatchInfo> = HashMap::new();

    // Process each row manually
    for row in rows {
        // Use try_get from SqlxRow trait
        let id: String = row.try_get("Id")?;
        let queue_id = row.try_get("QUEUE_ID")?;
        let win: bool = row.try_get("Win")?;
        let timestamp_ms: i64 = row.try_get("end_ts")?; // Use alias 'end_ts'
        let match_info_str: Option<String> = row.try_get("MatchInfo")?;

        // Parse timestamp
        let end_timestamp = DateTime::from_timestamp_millis(timestamp_ms)
            .ok_or_else(|| anyhow!("Invalid timestamp millis from DB: {}", timestamp_ms))?; // Use anyhow for error

        // Parse MatchInfo JSON string
        let match_info: Option<Match> = match match_info_str {
            Some(s) if !s.is_empty() => {
                match serde_json::from_str(&s) {
                    Result::Ok(m) => Some(m),
                    Err(e) => {
                        error!("Failed to parse MatchInfo JSON for match {}: {}", id, e);
                        // Decide how to handle parse errors: skip match, return error, etc.
                        // Here, we'll skip the match_info field for this entry.
                        None // Or return Err(anyhow!(...)) to fail the whole function
                    }
                }
            }
            _ => None,
        };

        // Insert into the result map
        seen_matches.insert(
            id.clone(),
            MatchInfo {
                id,
                queue_id,
                win,
                end_timestamp,
                match_info,
            },
        );
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
    let pool = &config.pool; // Get DB path from config

    let summoner_info = fetch_summoners(pool).await?;
    let summoner_names = summoner_info.values().cloned().collect::<HashSet<_>>();
    let puuids = fetch_puuids(pool, &summoner_names).await?;

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
    let seen_matches = fetch_seen_events(pool, &unique_match_ids).await?;
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
                    pool,
                    &match_info,
                    &mut queue_scores,
                    &queue_ids,
                    &puuids,
                    &mut pentakillers,
                    &mut dom_earners,
                    start_time,
                )
                .await?;
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

async fn update_match_info(
    pool: &sqlx::SqlitePool,
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
            pool,
            &match_info.metadata.match_id,
            queue_id,
            false,
            game_end_timestamp,
            match_info,
        )
        .await?;
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
        pool,
        &match_info.metadata.match_id,
        queue_id,
        is_win,
        game_end_timestamp,
        match_info,
    )
    .await?;

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

    let user_info = fetch_discord_usernames(&config.pool).await?;
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

    let user_info = fetch_discord_usernames(&config.pool).await?;
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

#[cfg(test)]
mod tests {
    use super::*; // Import items from outer scope
    use models::Score;
    use riven::consts::{Champion, PlatformRoute, Queue, Team as TeamId};
    use riven::models::match_v5::*;
    use std::collections::{HashMap, HashSet};

    // Helper to set up an in-memory database for testing
    async fn setup_in_memory_db_pool() -> Result<SqlitePool> {
        // Connect to an in-memory database
        let pool = SqlitePoolOptions::new().connect("sqlite::memory:").await?;

        // Run migrations (same schema setup)
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS User (
                 DiscordId TEXT PRIMARY KEY,
                 SummonerName TEXT NOT NULL UNIQUE,
                 DiscordUsername VARCHAR(255),
                 DiscordDisplayName VARCHAR(255)
             );",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS Summoner (
                 Id varchar(255),
                 AccountId varchar(255),
                 Puuid TEXT PRIMARY KEY,
                 Name TEXT NOT NULL UNIQUE
             );",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS MATCH (
                 Id TEXT PRIMARY KEY,
                 QUEUE_ID VARCHAR(255) NOT NULL,
                 Win BOOLEAN,
                 END_TIMESTAMP DATETIME,
                 MatchInfo BLOB
             );",
        )
        .execute(&pool)
        .await?;

        Ok(pool)
    }

    // Helper to create a basic Match struct for testing
    fn create_test_match(
        match_id: &str,
        queue_id: Queue,
        game_end_timestamp: Option<i64>,
        participants: Vec<Participant>,
        teams: Vec<riven::models::match_v5::Team>,
    ) -> Match {
        Match {
            metadata: Metadata {
                data_version: "2".to_string(),
                match_id: match_id.to_string(),
                participants: participants.iter().map(|p| p.puuid.clone()).collect(),
            },
            info: Info {
                game_creation: game_end_timestamp.unwrap_or(0) - 1800 * 1000, // Approx 30 mins before end
                game_duration: 1800,                                          // Approx 30 mins
                game_end_timestamp,
                game_id: 1234567890, // Example game ID
                game_mode: riven::consts::GameMode::CLASSIC,
                game_name: "test_game".to_string(),
                game_start_timestamp: game_end_timestamp.unwrap_or(0) - 1800 * 1000,
                game_type: Some(riven::consts::GameType::MATCHED_GAME),
                game_version: riven::consts::GameMode::CLASSIC.to_string(),
                map_id: if queue_id == Queue::HOWLING_ABYSS_5V5_ARAM {
                    riven::consts::Map::HOWLING_ABYSS
                } else {
                    riven::consts::Map::SUMMONERS_RIFT
                },
                participants,
                platform_id: PlatformRoute::NA1.to_string(),
                queue_id,
                teams,
                tournament_code: None,
                end_of_game_result: None,
            },
        }
    }

    // Helper to create a basic Participant struct
    #[allow(deprecated)]
    fn create_test_participant(
        puuid: &str,
        summoner_name: &str,
        team_id: TeamId,
        win: bool,
        kills: i32,
        deaths: i32,
        assists: i32,
        penta_kills: i32,
        placement: Option<i32>, // For Arena
        early_surrender: bool,
    ) -> Participant {
        Participant {
            puuid: puuid.to_string(),
            summoner_name: summoner_name.to_string(),
            team_id,
            win,
            kills,
            deaths,
            assists,
            penta_kills,
            placement,
            game_ended_in_early_surrender: early_surrender,
            game_ended_in_surrender: !win && !early_surrender, // Simple assumption
            missions: None,
            player_score0: None,
            player_score1: None,
            player_score2: None,
            player_score3: None,
            player_score4: None,
            player_score5: None,
            player_score6: None,
            player_score7: None,
            player_score8: None,
            player_score9: None,
            player_score10: None,
            player_score11: None,
            player_augment1: None,
            player_augment2: None,
            player_augment3: None,
            player_augment4: None,
            player_augment5: None,
            player_augment6: None,
            player_subteam_id: None,
            riot_id_game_name: None,
            subteam_placement: None,
            total_time_cc_dealt: 0,
            retreat_pings: Some(0),
            // Fill other fields with default/dummy values as needed for tests
            all_in_pings: Some(0),
            assist_me_pings: Some(0),
            bait_pings: Some(0),
            baron_kills: 0,
            basic_pings: Some(0),
            bounty_level: 0,
            challenges: None, // Add challenges if needed for specific tests
            champ_experience: 15000,
            champ_level: 18,
            champion_id: Result::Ok(Champion::ANNIE),
            champion_name: "Annie".to_string(),
            champion_transform: 0,
            command_pings: Some(0),
            consumables_purchased: 5,
            damage_dealt_to_buildings: Some(5000),
            damage_dealt_to_objectives: 8000,
            damage_dealt_to_turrets: 5000,
            damage_self_mitigated: 10000,
            danger_pings: Some(0),
            detector_wards_placed: 2,
            double_kills: if kills >= 2 { 1 } else { 0 },
            dragon_kills: 0,
            eligible_for_progression: Some(true),
            enemy_missing_pings: Some(0),
            enemy_vision_pings: Some(0),
            first_blood_assist: false,
            first_blood_kill: false,
            first_tower_assist: false,
            first_tower_kill: false,
            get_back_pings: Some(0),
            gold_earned: 12000,
            gold_spent: 11000,
            hold_pings: Some(0),
            individual_position: "MIDDLE".to_string(), // Example
            inhibitor_kills: 1,
            inhibitor_takedowns: Some(1),
            inhibitors_lost: Some(0),
            item0: 3020, // Example item ID
            item1: 3157,
            item2: 3089,
            item3: 3135,
            item4: 3165,
            item5: 3117,
            item6: 3340, // Trinket
            items_purchased: 15,
            killing_sprees: if kills >= 3 { 1 } else { 0 },
            lane: "MIDDLE".to_string(), // Example
            largest_critical_strike: 300,
            largest_killing_spree: kills,
            largest_multi_kill: if penta_kills > 0 {
                5
            } else if kills >= 2 {
                2
            } else {
                1
            },
            longest_time_spent_living: 600,
            magic_damage_dealt: 20000,
            magic_damage_dealt_to_champions: 15000,
            magic_damage_taken: 8000,
            need_vision_pings: Some(0),
            neutral_minions_killed: 20,
            nexus_kills: if win { 1 } else { 0 },
            nexus_lost: Some(if win { 0 } else { 1 }),
            nexus_takedowns: Some(if win { 1 } else { 0 }),
            objectives_stolen: 0,
            objectives_stolen_assists: 0,
            on_my_way_pings: Some(0),
            participant_id: 1, // Example
            perks: Perks {
                stat_perks: riven::models::match_v5::PerkStats {
                    defense: 5002,
                    flex: 5008,
                    offense: 5005,
                },
                styles: vec![], // Add perk styles if needed
            },
            physical_damage_dealt: 5000,
            physical_damage_dealt_to_champions: 3000,
            physical_damage_taken: 10000,
            profile_icon: 123,
            push_pings: Some(0),
            quadra_kills: 0,
            riot_id_name: None,
            riot_id_tagline: None,
            role: "SOLO".to_string(), // Example
            sight_wards_bought_in_game: 5,
            spell1_casts: 100,
            spell2_casts: 80,
            spell3_casts: 60,
            spell4_casts: 20,
            summoner1_casts: 5,
            summoner1_id: 4, // Flash
            summoner2_casts: 4,
            summoner2_id: 14, // Ignite
            summoner_id: "test_summoner_id".to_string(),
            summoner_level: 30,
            team_early_surrendered: early_surrender,
            team_position: "MIDDLE".to_string(), // Example
            time_c_cing_others: 10,
            time_played: 1800,
            total_ally_jungle_minions_killed: Some(5),
            total_damage_dealt: 25000,
            total_damage_dealt_to_champions: 18000,
            total_damage_shielded_on_teammates: 1000,
            total_damage_taken: 18000,
            total_enemy_jungle_minions_killed: Some(15),
            total_heal: 2000,
            total_heals_on_teammates: 500,
            total_minions_killed: 150,
            total_time_spent_dead: 60,
            total_units_healed: 5,
            triple_kills: 0,
            true_damage_dealt: 0,
            true_damage_dealt_to_champions: 0,
            true_damage_taken: 0,
            turret_kills: 2,
            turret_takedowns: Some(2),
            turrets_lost: Some(1),
            unreal_kills: 0,
            vision_cleared_pings: Some(0),
            vision_score: 25,
            vision_wards_bought_in_game: 3,
            wards_killed: 5,
            wards_placed: 10,
        }
    }

    // Helper to create a basic Team struct
    fn create_test_team(team_id: TeamId, win: bool) -> riven::models::match_v5::Team {
        riven::models::match_v5::Team {
            bans: vec![], // Add bans if needed
            objectives: riven::models::match_v5::Objectives {
                baron: Objective {
                    first: false,
                    kills: 0,
                },
                champion: Objective {
                    first: win,
                    kills: 20,
                }, // Example kills
                dragon: Objective {
                    first: false,
                    kills: 1,
                },
                inhibitor: Objective {
                    first: win,
                    kills: 1,
                },
                rift_herald: Objective {
                    first: false,
                    kills: 0,
                },
                tower: Objective {
                    first: win,
                    kills: 5,
                },
                horde: None,
                atakhan: None,
            },
            team_id,
            win,
            feats: None, // Add feats if needed
        }
    }

    #[test]
    fn test_mode_score_standard() {
        let score = Score {
            wins: 3,
            games: 5,
            warmup: 0,
            top_finishes: 0,
            bottom_finishes: 0,
        };
        assert_eq!(
            mode_score(&Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO, &score),
            "3\u{2006}-\u{2006}2"
        );
    }

    #[test]
    fn test_mode_score_warmup() {
        let score = Score {
            wins: 0,
            games: 0,
            warmup: 1,
            top_finishes: 0,
            bottom_finishes: 0,
        };
        assert_eq!(
            mode_score(&Queue::SUMMONERS_RIFT_5V5_DRAFT_PICK, &score),
            "🏃"
        );
        // Warmup shouldn't show if there are games played
        let score_with_games = Score {
            wins: 0,
            games: 1,
            warmup: 1,
            top_finishes: 0,
            bottom_finishes: 0,
        };
        assert_eq!(
            mode_score(&Queue::SUMMONERS_RIFT_5V5_DRAFT_PICK, &score_with_games),
            "0\u{2006}-\u{2006}1"
        );
        // Warmup shouldn't show for Clash
        assert_eq!(
            mode_score(&Queue::SUMMONERS_RIFT_CLASH, &score),
            "0\u{2006}-\u{2006}0"
        );
    }

    #[test]
    fn test_mode_score_arena() {
        let score = Score {
            wins: 2,
            games: 5, // games field might be used differently for Arena internally, but mode_score uses wins/top/bottom
            warmup: 0,
            top_finishes: 3, // e.g., 2 wins (1st) + 1 top 4 finish
            bottom_finishes: 2,
        };
        assert_eq!(
            mode_score(&Queue::ARENA_2V2V2V2_CHERRY, &score),
            "2\u{2006}-\u{2006}3\u{2006}-\u{2006}2"
        );
    }

    #[tokio::test] // Mark as async test
    async fn test_fetch_summoners_db() -> Result<()> {
        let pool = setup_in_memory_db_pool().await?; // Use async helper
        sqlx::query("INSERT INTO User (DiscordId, SummonerName) VALUES (?1, ?2), (?3, ?4)")
            .bind("111")
            .bind("PlayerOne")
            .bind("222")
            .bind("PlayerTwo")
            .execute(&pool)
            .await?;

        // Call the actual async function
        let summoners = fetch_summoners(&pool).await?;

        assert_eq!(summoners.len(), 2);
        assert_eq!(summoners.get(&111), Some(&"PlayerOne".to_string()));
        assert_eq!(summoners.get(&222), Some(&"PlayerTwo".to_string()));
        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_puuids_db() -> Result<()> {
        let pool = setup_in_memory_db_pool().await?;
        sqlx::query("INSERT INTO summoner (puuid, name) VALUES (?1, ?2), (?3, ?4), (?5, ?6)")
            .bind("puuid1")
            .bind("playerone") // Keep names lowercase in DB for consistency
            .bind("puuid2")
            .bind("playertwo")
            .bind("puuid3")
            .bind("playerthree")
            .execute(&pool)
            .await?;

        let summoner_names_to_fetch: HashSet<String> = HashSet::from([
            "playerone".to_string(), // Use lowercase to match DB query logic
            "playertwo".to_string(),
            "playerfour".to_string(), // Non-existent
        ]);

        // Call the actual async function
        let puuids = fetch_puuids(&pool, &summoner_names_to_fetch).await?;

        assert_eq!(puuids.len(), 2);
        assert!(puuids.contains("puuid1"));
        assert!(puuids.contains("puuid2"));
        assert!(!puuids.contains("puuid3"));
        Ok(())
    }

    #[tokio::test]
    async fn test_update_and_fetch_match_db() -> Result<()> {
        let pool = setup_in_memory_db_pool().await?;
        let now = chrono::Local::now().timestamp_millis();
        let match_id = "NA1_TESTMATCH1".to_string();
        let queue_id = Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO;
        let is_win = true;

        let participant1 = create_test_participant(
            "puuid1",
            "Player1",
            TeamId::BLUE,
            true,
            5,
            2,
            10,
            0,
            None,
            false,
        );
        let team1 = create_test_team(TeamId::BLUE, true);
        let team2 = create_test_team(TeamId::RED, false);
        let test_match = create_test_match(
            &match_id,
            queue_id,
            Some(now),
            vec![participant1],
            vec![team1, team2],
        );

        // Call the actual async update function
        update_match(
            &pool,
            &match_id,
            i64::from(u16::from(queue_id)),
            is_win,
            now,
            &test_match,
        )
        .await?;

        // Call the actual async fetch function
        let matches_to_fetch = vec![match_id.clone()];
        let seen_matches_map = fetch_seen_events(&pool, &matches_to_fetch).await?;

        assert_eq!(seen_matches_map.len(), 1);
        let fetched_info = seen_matches_map.get(&match_id).unwrap();

        assert_eq!(fetched_info.id, match_id);
        assert_eq!(
            fetched_info.queue_id,
            i64::from(u16::from(queue_id)).to_string()
        );
        assert_eq!(fetched_info.win, is_win);
        assert_eq!(fetched_info.end_timestamp.timestamp_millis(), now);
        assert!(fetched_info.match_info.is_some());
        assert_eq!(
            fetched_info.match_info.as_ref().unwrap().metadata.match_id,
            match_id
        );

        Ok(())
    }

    // --- update_match_info tests need #[tokio::test] and .await ---
    // They also need a dummy pool, even if update_match isn't the primary focus

    #[tokio::test]
    async fn test_update_match_info_standard_win() -> Result<()> {
        let pool = setup_in_memory_db_pool().await?; // Need pool for update_match call
        let mut queue_scores: HashMap<Queue, Score> = HashMap::new();
        let queue_ids: HashMap<Queue, &str> =
            HashMap::from([(Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO, "Solo")]);
        let puuids: HashSet<String> = HashSet::from(["puuid1".to_string()]);
        let mut pentakillers = Vec::new();
        let mut dom_earners = Vec::new();
        let start_time = chrono::Local::now().timestamp_millis() - 1000 * 60 * 60;

        let p1 = create_test_participant(
            "puuid1",
            "Player1",
            TeamId::BLUE,
            true,
            10,
            2,
            5,
            0,
            None,
            false,
        );
        let p2 = create_test_participant(
            "puuid2",
            "Player2",
            TeamId::RED,
            false,
            2,
            10,
            3,
            0,
            None,
            false,
        );
        let team1 = create_test_team(TeamId::BLUE, true);
        let team2 = create_test_team(TeamId::RED, false);
        let match_info = create_test_match(
            "NA1_WIN",
            Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO,
            Some(start_time + 1000 * 30),
            vec![p1, p2],
            vec![team1, team2],
        );

        // Call async function with await
        update_match_info(
            &pool,
            &match_info,
            &mut queue_scores,
            &queue_ids,
            &puuids,
            &mut pentakillers,
            &mut dom_earners,
            start_time,
        )
        .await?;

        let score = queue_scores
            .get(&Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO)
            .unwrap();
        assert_eq!(score.wins, 1);
        assert_eq!(score.games, 1);
        assert_eq!(score.warmup, 0);
        assert!(pentakillers.is_empty());
        assert!(dom_earners.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_update_match_info_standard_loss_warmup() -> Result<()> {
        let pool = setup_in_memory_db_pool().await?;
        let mut queue_scores: HashMap<Queue, Score> = HashMap::new();
        let queue_ids: HashMap<Queue, &str> =
            HashMap::from([(Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO, "Solo")]);
        let puuids: HashSet<String> = HashSet::from(["puuid1".to_string()]);
        let mut pentakillers = Vec::new();
        let mut dom_earners = Vec::new();
        let start_time = chrono::Local::now().timestamp_millis() - 1000 * 60 * 60;

        let p1 = create_test_participant(
            "puuid1",
            "Player1",
            TeamId::BLUE,
            false,
            2,
            10,
            5,
            0,
            None,
            false,
        );
        let p2 = create_test_participant(
            "puuid2",
            "Player2",
            TeamId::RED,
            true,
            10,
            2,
            3,
            0,
            None,
            false,
        );
        let team1 = create_test_team(TeamId::BLUE, false);
        let team2 = create_test_team(TeamId::RED, true);
        let match_info = create_test_match(
            "NA1_LOSS",
            Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO,
            Some(start_time + 1000 * 30),
            vec![p1, p2],
            vec![team1, team2],
        );

        update_match_info(
            &pool,
            &match_info,
            &mut queue_scores,
            &queue_ids,
            &puuids,
            &mut pentakillers,
            &mut dom_earners,
            start_time,
        )
        .await?;

        let score = queue_scores
            .get(&Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO)
            .unwrap();
        assert_eq!(score.wins, 0);
        assert_eq!(score.games, 0);
        assert_eq!(score.warmup, 1);

        let p3 = create_test_participant(
            "puuid1",
            "Player1",
            TeamId::BLUE,
            false,
            3,
            8,
            6,
            0,
            None,
            false,
        );
        let p4 = create_test_participant(
            "puuid3",
            "Player3",
            TeamId::RED,
            true,
            8,
            3,
            2,
            0,
            None,
            false,
        );
        let team3 = create_test_team(TeamId::BLUE, false);
        let team4 = create_test_team(TeamId::RED, true);
        let match_info2 = create_test_match(
            "NA1_LOSS2",
            Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO,
            Some(start_time + 1000 * 60),
            vec![p3, p4],
            vec![team3, team4],
        );

        update_match_info(
            &pool,
            &match_info2,
            &mut queue_scores,
            &queue_ids,
            &puuids,
            &mut pentakillers,
            &mut dom_earners,
            start_time,
        )
        .await?;

        let score_after_2 = queue_scores
            .get(&Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO)
            .unwrap();
        assert_eq!(score_after_2.wins, 0);
        assert_eq!(score_after_2.games, 0); // Still 0 games
        assert_eq!(score_after_2.warmup, 2); // Warmup increases

        let p5 = create_test_participant(
            "puuid1",
            "Player1",
            TeamId::BLUE,
            false,
            1,
            11,
            7,
            0,
            None,
            false,
        );
        let p6 = create_test_participant(
            "puuid4",
            "Player4",
            TeamId::RED,
            true,
            11,
            1,
            1,
            0,
            None,
            false,
        );
        let team5 = create_test_team(TeamId::BLUE, false);
        let team6 = create_test_team(TeamId::RED, true);
        let match_info3 = create_test_match(
            "NA1_LOSS3",
            Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO,
            Some(start_time + 1000 * 90),
            vec![p5, p6],
            vec![team5, team6],
        );

        update_match_info(
            &pool,
            &match_info3,
            &mut queue_scores,
            &queue_ids,
            &puuids,
            &mut pentakillers,
            &mut dom_earners,
            start_time,
        )
        .await?;

        let score_after_3 = queue_scores
            .get(&Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO)
            .unwrap();
        assert_eq!(score_after_3.wins, 0);
        assert_eq!(score_after_3.games, 1); // Now counts as a game
        assert_eq!(score_after_3.warmup, 2); // Warmup count stays at max

        Ok(())
    }

    #[tokio::test]
    async fn test_update_match_info_penta() -> Result<()> {
        let pool = setup_in_memory_db_pool().await?;
        let mut queue_scores: HashMap<Queue, Score> = HashMap::new();
        let queue_ids: HashMap<Queue, &str> =
            HashMap::from([(Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO, "Solo")]);
        let puuids: HashSet<String> = HashSet::from(["puuid_penta".to_string()]);
        let mut pentakillers = Vec::new();
        let mut dom_earners = Vec::new();
        let start_time = chrono::Local::now().timestamp_millis() - 1000 * 60 * 60;

        let p1 = create_test_participant(
            "puuid_penta",
            "Penta Pete",
            TeamId::BLUE,
            true,
            25,
            5,
            5,
            1,
            None,
            false,
        );
        let p2 = create_test_participant(
            "puuid_other",
            "Feeder Fred",
            TeamId::RED,
            false,
            5,
            25,
            0,
            0,
            None,
            false,
        );
        let team1 = create_test_team(TeamId::BLUE, true);
        let team2 = create_test_team(TeamId::RED, false);
        let match_info = create_test_match(
            "NA1_PENTA",
            Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO,
            Some(start_time + 1000 * 30),
            vec![p1, p2],
            vec![team1, team2],
        );

        update_match_info(
            &pool,
            &match_info,
            &mut queue_scores,
            &queue_ids,
            &puuids,
            &mut pentakillers,
            &mut dom_earners,
            start_time,
        )
        .await?;

        let score = queue_scores
            .get(&Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO)
            .unwrap();
        assert_eq!(score.wins, 1);
        assert_eq!(score.games, 1);
        assert_eq!(pentakillers.len(), 1);
        assert_eq!(pentakillers[0].1, "Penta Pete");
        assert!(dom_earners.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_update_match_info_doms() -> Result<()> {
        let pool = setup_in_memory_db_pool().await?;
        let mut queue_scores: HashMap<Queue, Score> = HashMap::new();
        let queue_ids: HashMap<Queue, &str> =
            HashMap::from([(Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO, "Solo")]);
        let puuids: HashSet<String> = HashSet::from(["puuid_doms".to_string()]);
        let mut pentakillers = Vec::new();
        let mut dom_earners = Vec::new();
        let start_time = chrono::Local::now().timestamp_millis() - 1000 * 60 * 60;

        let p1 = create_test_participant(
            "puuid_doms",
            "Dominic",
            TeamId::BLUE,
            true,
            5,
            5,
            5,
            0,
            None,
            false,
        );
        let p2 = create_test_participant(
            "puuid_other",
            "Average Andy",
            TeamId::RED,
            false,
            5,
            5,
            5,
            0,
            None,
            false,
        );
        let team1 = create_test_team(TeamId::BLUE, true);
        let team2 = create_test_team(TeamId::RED, false);
        let match_info = create_test_match(
            "NA1_DOMS",
            Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO,
            Some(start_time + 1000 * 30),
            vec![p1, p2],
            vec![team1, team2],
        );

        update_match_info(
            &pool,
            &match_info,
            &mut queue_scores,
            &queue_ids,
            &puuids,
            &mut pentakillers,
            &mut dom_earners,
            start_time,
        )
        .await?;

        let score = queue_scores
            .get(&Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO)
            .unwrap();
        assert_eq!(score.wins, 1);
        assert_eq!(score.games, 1);
        assert!(pentakillers.is_empty());
        assert_eq!(dom_earners.len(), 1);
        assert_eq!(dom_earners[0].1, "Dominic");
        Ok(())
    }

    #[tokio::test]
    async fn test_update_match_info_arena() -> Result<()> {
        let pool = setup_in_memory_db_pool().await?;
        let mut queue_scores: HashMap<Queue, Score> = HashMap::new();
        let queue_ids: HashMap<Queue, &str> =
            HashMap::from([(Queue::ARENA_2V2V2V2_CHERRY, "Arena")]);
        let puuids: HashSet<String> =
            HashSet::from(["puuid_arena1".to_string(), "puuid_arena2".to_string()]);
        let mut pentakillers = Vec::new();
        let mut dom_earners = Vec::new();
        let start_time = chrono::Local::now().timestamp_millis() - 1000 * 60 * 60;

        let p1 = create_test_participant(
            "puuid_arena1",
            "Arena Ace",
            TeamId::BLUE,
            true,
            10,
            2,
            5,
            0,
            Some(1),
            false,
        );
        let p2 = create_test_participant(
            "puuid_arena2",
            "Arena Ally",
            TeamId::BLUE,
            true,
            8,
            3,
            6,
            0,
            Some(1),
            false,
        );
        let p3 = create_test_participant(
            "puuid_other1",
            "Rival 1",
            TeamId::RED,
            false,
            5,
            5,
            2,
            0,
            Some(3),
            false,
        );
        let p4 = create_test_participant(
            "puuid_other2",
            "Rival 2",
            TeamId::RED,
            false,
            4,
            6,
            5,
            0,
            Some(3),
            false,
        );
        let p5 = create_test_participant(
            "puuid_arena_opponent",
            "Bottom Feeder",
            TeamId::OTHER,
            false,
            2,
            10,
            1,
            0,
            Some(7),
            false,
        );
        let team_dummy = create_test_team(TeamId::BLUE, true);

        let match_info = create_test_match(
            "NA1_ARENA",
            Queue::ARENA_2V2V2V2_CHERRY,
            Some(start_time + 1000 * 30),
            vec![p1, p2, p3, p4, p5],
            vec![team_dummy.clone()],
        );

        update_match_info(
            &pool,
            &match_info,
            &mut queue_scores,
            &queue_ids,
            &puuids,
            &mut pentakillers,
            &mut dom_earners,
            start_time,
        )
        .await?;

        let score = queue_scores.get(&Queue::ARENA_2V2V2V2_CHERRY).unwrap();
        assert_eq!(score.wins, 1);
        assert_eq!(score.games, 1);
        assert_eq!(score.top_finishes, 1); // Both players were 1st, counts as one top finish
        assert_eq!(score.bottom_finishes, 0);

        let p6 = create_test_participant(
            "puuid_arena1",
            "Arena Ace",
            TeamId::BLUE,
            false,
            3,
            8,
            2,
            0,
            Some(6),
            false,
        );
        let p7 = create_test_participant(
            "puuid_arena2",
            "Arena Ally",
            TeamId::BLUE,
            false,
            2,
            9,
            1,
            0,
            Some(3),
            false,
        ); // One top 4, one bottom
        let match_info2 = create_test_match(
            "NA1_ARENA_LOSS",
            Queue::ARENA_2V2V2V2_CHERRY,
            Some(start_time + 1000 * 60),
            vec![p6, p7],
            vec![team_dummy.clone()],
        );

        update_match_info(
            &pool,
            &match_info2,
            &mut queue_scores,
            &queue_ids,
            &puuids,
            &mut pentakillers,
            &mut dom_earners,
            start_time,
        )
        .await?;

        let score_after_2 = queue_scores.get(&Queue::ARENA_2V2V2V2_CHERRY).unwrap();
        assert_eq!(score_after_2.wins, 1);
        assert_eq!(score_after_2.games, 2);
        assert_eq!(score_after_2.top_finishes, 2); // Player 7 got 3rd place
        assert_eq!(score_after_2.bottom_finishes, 1); // Player 6 got 6th place
        Ok(())
    }

    #[tokio::test]
    async fn test_update_match_info_early_surrender() -> Result<()> {
        let pool = setup_in_memory_db_pool().await?;
        let mut queue_scores: HashMap<Queue, Score> = HashMap::new();
        let queue_ids: HashMap<Queue, &str> =
            HashMap::from([(Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO, "Solo")]);
        let puuids: HashSet<String> = HashSet::from(["puuid1".to_string()]);
        let mut pentakillers = Vec::new();
        let mut dom_earners = Vec::new();
        let start_time = chrono::Local::now().timestamp_millis() - 1000 * 60 * 60;

        let p1 = create_test_participant(
            "puuid1",
            "Player1",
            TeamId::BLUE,
            false,
            0,
            1,
            0,
            0,
            None,
            true,
        );
        let p2 = create_test_participant(
            "puuid2",
            "Player2",
            TeamId::RED,
            true,
            1,
            0,
            0,
            0,
            None,
            false,
        );
        let team1 = create_test_team(TeamId::BLUE, false);
        let team2 = create_test_team(TeamId::RED, true);
        let match_info = create_test_match(
            "NA1_EARLY_FF",
            Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO,
            Some(start_time + 1000 * 30),
            vec![p1, p2],
            vec![team1, team2],
        );

        update_match_info(
            &pool,
            &match_info,
            &mut queue_scores,
            &queue_ids,
            &puuids,
            &mut pentakillers,
            &mut dom_earners,
            start_time,
        )
        .await?;

        assert!(
            queue_scores
                .get(&Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO)
                .is_none()
                || queue_scores
                    .get(&Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO)
                    .unwrap()
                    .games
                    == 0
        );
        assert!(pentakillers.is_empty());
        assert!(dom_earners.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_update_match_info_timestamp_before_start() -> Result<()> {
        let pool = setup_in_memory_db_pool().await?;
        let mut queue_scores: HashMap<Queue, Score> = HashMap::new();
        let queue_ids: HashMap<Queue, &str> =
            HashMap::from([(Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO, "Solo")]);
        let puuids: HashSet<String> = HashSet::from(["puuid1".to_string()]);
        let mut pentakillers = Vec::new();
        let mut dom_earners = Vec::new();
        let start_time = chrono::Local::now().timestamp_millis();

        let p1 = create_test_participant(
            "puuid1",
            "Player1",
            TeamId::BLUE,
            true,
            10,
            2,
            5,
            0,
            None,
            false,
        );
        let p2 = create_test_participant(
            "puuid2",
            "Player2",
            TeamId::RED,
            false,
            2,
            10,
            3,
            0,
            None,
            false,
        );
        let team1 = create_test_team(TeamId::BLUE, true);
        let team2 = create_test_team(TeamId::RED, false);
        let match_info = create_test_match(
            "NA1_OLD_MATCH",
            Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO,
            Some(start_time - 1000 * 60 * 60),
            vec![p1, p2],
            vec![team1, team2],
        );

        update_match_info(
            &pool,
            &match_info,
            &mut queue_scores,
            &queue_ids,
            &puuids,
            &mut pentakillers,
            &mut dom_earners,
            start_time,
        )
        .await?;

        assert!(queue_scores
            .get(&Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO)
            .is_none());
        assert!(pentakillers.is_empty());
        assert!(dom_earners.is_empty());
        Ok(())
    }

    // format_highlight_names test remains synchronous
    fn format_highlight_names(names: &[String]) -> String {
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

    #[test]
    fn test_format_highlight_names() {
        assert_eq!(format_highlight_names(&[]), "");
        assert_eq!(format_highlight_names(&["Alice".to_string()]), "Alice");
        assert_eq!(
            format_highlight_names(&["Alice".to_string(), "Bob".to_string()]),
            "Alice and Bob"
        );
        assert_eq!(
            format_highlight_names(&[
                "Alice".to_string(),
                "Bob".to_string(),
                "Charlie".to_string()
            ]),
            "Alice, Bob, and Charlie"
        );
    }
}
