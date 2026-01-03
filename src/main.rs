use core::panic;
use std::collections::HashMap;
use std::collections::HashSet;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::models::MatchInfo;
use crate::riot_utils::GameId;
use anyhow::Result;
use anyhow::{Context, Error, Ok, anyhow};
use chrono::Timelike;
use chrono::{Datelike, TimeZone};
use dotenvy::dotenv;
use futures::future::FutureExt;
use futures::select;
use futures::stream;
use futures_util::StreamExt;
#[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;
use models::Score;
use riven::RiotApi;
use riven::consts::Team;
use riven::consts::{Queue, RegionalRoute};
use riven::models::match_v5::Match;
use riven::models::tft_match_v1::Match as TftMatch;
use serenity::all::CreateInteractionResponse;
use serenity::all::CreateInteractionResponseMessage;
use serenity::all::CreateMessage;
use serenity::all::GuildId;
use serenity::async_trait;
use serenity::model::prelude::Member;
use serenity::model::{id::ChannelId, prelude::Interaction};
use serenity::prelude::*;
use sqlx::sqlite::SqlitePoolOptions;
use tokio::sync::RwLock;
use tokio::task;
use tokio::time;
use tracing::{error, info};
use tracing_subscriber::{Layer, filter, prelude::*};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

mod commands;
mod db;
mod models;
mod riot_utils;

use riot_utils::riot_api;

#[derive(Debug, Clone)]
struct Config {
    riot_api_token: String,
    tft_riot_api_token: String,
    discord_bot_token: String,
    voice_channel_id: ChannelId,
    tft_voice_channel_id: ChannelId,
    updates_channel_id: ChannelId,
    discord_guild_id: GuildId,
    log_path: PathBuf,
    pool: sqlx::SqlitePool,
}

async fn load_config() -> Result<Config> {
    dotenv().ok();

    let riot_api_token = env::var("RIOT_API_TOKEN").context("Missing RIOT_API_TOKEN")?;
    let discord_bot_token = env::var("DISCORD_BOT_TOKEN").context("Missing DISCORD_BOT_TOKEN")?;

    let tft_riot_api_token = env::var("TFT_RIOT_API_TOKEN")
        .context("Missing TFT_RIOT_API_TOKEN")
        .unwrap_or_default();

    let voice_channel_id_u64 = env::var("VOICE_CHANNEL_ID")
        .context("Missing VOICE_CHANNEL_ID")?
        .parse::<u64>()
        .context("Invalid VOICE_CHANNEL_ID (must be u64)")?;
    let voice_channel_id = ChannelId::from(voice_channel_id_u64);

    let tft_voice_channel_id_u64 = env::var("TFT_VOICE_CHANNEL_ID")
        .context("Missing TFT_VOICE_CHANNEL_ID")?
        .parse::<u64>()
        .context("Invalid TFT_VOICE_CHANNEL_ID (must be u64)")?;
    let tft_voice_channel_id = ChannelId::from(tft_voice_channel_id_u64);

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

    let db_path_str = env::var("DB_PATH").unwrap_or(String::from("sqlite://sqlite.db"));

    let pool = SqlitePoolOptions::new()
        .max_connections(5) // Configure pool size
        .connect(&db_path_str) // Use database_url from config
        .await
        .context("Failed to create SQLite connection pool")?;

    Ok(Config {
        riot_api_token,
        tft_riot_api_token,
        discord_bot_token,
        voice_channel_id,
        tft_voice_channel_id,
        updates_channel_id,
        discord_guild_id,
        log_path,
        pool,
    })
}

struct Handler {
    users: Arc<RwLock<HashSet<u64>>>,
    current_status: Arc<RwLock<String>>,
    discord_auth: Arc<serenity::http::Http>,
    config: Arc<Config>, // Add config here
}

impl Handler {
    async fn remove_user(&self, member: &Member) {
        let mut users = self.users.write().await;
        users.remove(&member.user.id.get());
        if users.is_empty() {
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
    if *queue_id == Queue::ARENA_2V2V2V2_CHERRY || *queue_id == Queue::CONVERGENCE_TEAMFIGHT_TACTICS
    {
        return format!(
            "{}\u{2006}-\u{2006}{}\u{2006}-\u{2006}{}",
            score.wins, score.top_finishes, score.bottom_finishes,
        );
    } else if score.warmup > 0 && score.games == 0 && *queue_id != Queue::SUMMONERS_RIFT_CLASH {
        return "🏃".to_string();
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
                        users.insert(member.user.id.get());
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
                commands::rank::register(),
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

            let message = match command.data.name.as_str() {
                "groups" => {
                    commands::groups::run(&ctx, &command.data.options, self.config.clone()).await
                }
                "globetrotters" => commands::globetrotters::run(&ctx, &command.data.options),
                "register" => {
                    commands::register::run(&ctx, &command.data.options, self.config.clone()).await
                }
                "rank" => {
                    commands::rank::run(
                        &ctx,
                        &command.data.options,
                        self.config.clone(),
                        &command.user,
                    )
                    .await
                }
                x => Err(anyhow!(format!("{x} is not implemented :("))),
            };

            if let Err(why) = command
                .create_response(
                    &ctx.http,
                    CreateInteractionResponse::Message(message.unwrap_or_else(|error| {
                        info!("Error: {}", error);
                        CreateInteractionResponseMessage::new().content(format!(
                            "An error occurred while processing your command: {}",
                            error
                        ))
                    })),
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
                    users.insert(member.user.id.get());
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
                    users.insert(member.user.id.get());
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
    let tft_riot_api = RiotApi::new(&config.tft_riot_api_token);

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

    let active_users: Arc<RwLock<HashSet<u64>>> = Arc::new(RwLock::new(HashSet::new()));
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
    let background_tft_riot_api = Arc::new(tft_riot_api);
    let background_discord_client = discord_client.clone();
    let background_current_status = current_status.clone();

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
        let summoners = db::fetch_summoners(pool).await?;
        let active_summoner_names = summoners.values().cloned().collect::<HashSet<_>>(); // Collect names for puuid fetch

        loop {
            let summoner_puuids = db::fetch_summoner_puuids(pool, &active_summoner_names).await?;

            let new_start_time = get_start_time()?;
            let is_new_day = current_date < new_start_time;

            if is_new_day {
                info!(
                    "It's a new day, time to scan histories at least once. Current day was {}, now it's {}",
                    current_date, new_start_time
                );
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

                if &config.tft_riot_api_token != "" {
                    match check_tft_match_history(
                        &background_riot_api,
                        &background_discord_client,
                        &background_current_status,
                        &background_config,
                    )
                    .await
                    {
                        Result::Ok(()) => {}
                        Result::Err(e) => error!("Error during daily TFT history check: {}", e),
                    }
                    start_time = chrono::Local::now().timestamp(); // Reset scan window start
                    info!(
                        "Daily check done. Setting scan start time to {}",
                        start_time
                    );
                    current_date = new_start_time; // Update the date marker
                }
            }

            let mut found_new_matches = false;
            let mut found_new_tft_matches = false;
            for (lol_puuid, tft_puuid) in summoner_puuids.values() {
                interval.tick().await; // Rate limit per PUUID check
                if !found_new_matches {
                    let matches_fetch: Result<Vec<String>> = riot_api!(
                        background_riot_api // Use the API client from the closure
                            .match_v5()
                            .get_match_ids_by_puuid(
                                RegionalRoute::AMERICAS,
                                lol_puuid,
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
                        }
                        Result::Ok(_) => { /* No new matches for this user */ }
                        Result::Err(e) => {
                            error!(
                                "Hit an error while fetching matches for {}: {}",
                                lol_puuid, e
                            )
                        }
                    }
                }

                if !found_new_tft_matches
                    && &background_config.tft_riot_api_token != ""
                    && let Some(puuid_to_use) = tft_puuid
                {
                    let tft_matches_fetch: Result<Vec<String>> = riot_api!(
                        background_tft_riot_api
                            .tft_match_v1()
                            .get_match_ids_by_puuid(
                                RegionalRoute::AMERICAS,
                                puuid_to_use,
                                None,
                                None,
                                None,
                                Some(start_time), // Check only since last scan OR daily reset
                            )
                            .await
                    );

                    match tft_matches_fetch {
                        Result::Ok(match_list) if !match_list.is_empty() => {
                            info!(
                                "Found {} new TFT match(es) for a user since timestamp {}. Triggering full scan.",
                                match_list.len(),
                                start_time
                            );
                            found_new_tft_matches = true;
                        }
                        Result::Ok(_) => { /* No new matches for this user */ }
                        Result::Err(e) => {
                            error!(
                                "Hit an error while fetching TFT matches for {}: {}",
                                puuid_to_use, e
                            )
                        }
                    }
                }

                if found_new_matches
                    && (found_new_tft_matches || &background_config.tft_riot_api_token == "")
                {
                    break;
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

            if found_new_tft_matches {
                match check_tft_match_history(
                    &background_tft_riot_api,
                    &background_discord_client,
                    &background_current_status,
                    &background_config,
                )
                .await
                {
                    Result::Ok(()) => {}
                    Result::Err(e) => error!("Error during triggered TFT history check: {}", e),
                }
                start_time = chrono::Local::now().timestamp();
                info!(
                    "Triggered TFT check done. Setting scan start time to {}",
                    start_time
                );
            }

            // If it wasn't a new day and no new matches were found, just wait for the next interval cycle
            if !is_new_day && !found_new_matches && !found_new_tft_matches {
                interval.tick().await; // Ensure we wait at least the interval duration even if loop was fast
            }
        }
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
        (Queue(2300), "Brawl"),
        (Queue::HOWLING_ABYSS_5V5_ARAM, "ARAM"),
        (Queue::SUMMONERS_RIFT_CLASH, "Clash"),
        (Queue::ARENA_2V2V2V2_CHERRY, "Arena"),
    ]);
    let mut queue_scores: HashMap<Queue, Score> = HashMap::new();
    let start_time = get_start_time()?;
    let pool = &config.pool; // Get DB path from config

    let summoner_info = db::fetch_summoners(pool).await?;
    let summoner_names = summoner_info.values().cloned().collect::<HashSet<_>>();
    let puuids = db::fetch_puuids(pool, &summoner_names).await?;

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
    let seen_matches = db::fetch_seen_events(pool, &unique_match_ids).await?;
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

async fn check_tft_match_history(
    riot_api: &RiotApi,
    discord_client: &Arc<serenity::http::Http>,
    current_status: &Arc<RwLock<String>>,
    config: &Config,
) -> Result<()> {
    let queue_ids: HashMap<Queue, &str> =
        HashMap::from([(Queue::CONVERGENCE_TEAMFIGHT_TACTICS, "TFT")]);
    let mut queue_scores: HashMap<Queue, Score> = HashMap::new();
    let start_time = get_start_time()?;
    let pool = &config.pool;

    let summoner_info = db::fetch_summoners(pool).await?;
    let summoner_names = summoner_info.values().cloned().collect::<HashSet<_>>();
    let puuids = db::fetch_tft_puuids(pool, &summoner_names).await?;

    let mut all_match_ids: Vec<String> = Vec::new();
    let matches_requests = stream::iter(puuids.clone())
        .map(|puuid| async move {
            riot_api!(
                riot_api
                    .tft_match_v1()
                    .get_match_ids_by_puuid(
                        RegionalRoute::AMERICAS,
                        &puuid,
                        None,
                        None,
                        None,
                        Some(start_time),
                    )
                    .await
            )
        })
        .buffer_unordered(5);

    let matches_list: Vec<Result<Vec<String>>> = matches_requests.collect().await;

    for result in matches_list {
        match result {
            Result::Ok(match_ids) => {
                all_match_ids.extend(match_ids);
            }
            Result::Err(e) => {
                error!("Error fetching TFT match IDs for a PUUID: {:?}", e);
            }
        }
    }

    let mut unique_match_ids: Vec<String> = all_match_ids
        .into_iter()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    unique_match_ids.sort();

    if unique_match_ids.is_empty() {
        info!(
            "No relevant TFT matches found since start time {}",
            start_time
        );
        let results = String::from("No TFT games yet!");
        update_discord_status(
            &results,
            Some(current_status),
            discord_client,
            config.tft_voice_channel_id,
        )
        .await?;
        return Ok(());
    }

    let seen_matches = fetch_seen_tft_events(pool, &unique_match_ids).await?;

    let mut matches_to_fetch_details = Vec::new();
    for match_id in &unique_match_ids {
        if let Some(match_info) = seen_matches.get(match_id) {
            process_seen_tft_match_score(match_info, &mut queue_scores, &puuids)?;
        } else {
            matches_to_fetch_details.push(match_id.clone());
        }
    }

    let match_detail_requests = stream::iter(matches_to_fetch_details)
        .map(|match_id| async move {
            let result = riot_api!(
                riot_api
                    .tft_match_v1()
                    .get_match(RegionalRoute::AMERICAS, &match_id)
                    .await
            );
            Ok((match_id, result))
        })
        .buffer_unordered(10);

    let match_details: Vec<(String, Result<Option<TftMatch>, Error>)> = match_detail_requests
        .collect::<Vec<Result<(String, Result<Option<TftMatch>, Error>), Error>>>()
        .await
        .into_iter()
        .filter_map(Result::ok)
        .collect();

    for (match_id, result) in match_details {
        match result {
            Result::Ok(Some(match_info)) => {
                update_tft_match_info(pool, &match_info, &mut queue_scores, &puuids, start_time)
                    .await?;
            }
            Result::Ok(None) => {
                error!("Riot API returned Ok(None) for TFT match id {}", match_id);
            }
            Result::Err(e) => {
                error!(
                    "Failed to fetch details for TFT match {}: {:?}",
                    match_id, e
                );
            }
        }
    }

    let status_message = generate_status_message(&queue_scores, &queue_ids);

    update_discord_status(
        &status_message,
        Some(current_status),
        discord_client,
        config.voice_channel_id,
    )
    .await
}

async fn fetch_seen_tft_events(
    pool: &sqlx::SqlitePool,
    match_ids: &[String],
) -> Result<HashMap<String, TftMatch>> {
    if match_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let query_string = format!(
        "SELECT Id, MatchInfo FROM MATCH WHERE Id IN ({})",
        match_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",")
    );

    let mut query = sqlx::query_as::<_, (String, Vec<u8>)>(&query_string);
    for id in match_ids {
        query = query.bind(id);
    }

    let rows = query.fetch_all(pool).await?;

    let mut results = HashMap::new();
    for (id, blob) in rows {
        if let Result::Ok(match_info) = serde_json::from_slice::<TftMatch>(&blob) {
            results.insert(id, match_info);
        }
    }
    Ok(results)
}

async fn save_tft_match(
    pool: &sqlx::SqlitePool,
    match_id: &str,
    queue_id: i64,
    win: bool,
    timestamp: i64,
    match_info: &TftMatch,
) -> Result<()> {
    let blob = serde_json::to_vec(match_info)?;
    sqlx::query(
        "INSERT OR REPLACE INTO MATCH (Id, QUEUE_ID, Win, END_TIMESTAMP, MatchInfo) VALUES (?, ?, ?, ?, ?)"
    )
    .bind(match_id)
    .bind(queue_id.to_string())
    .bind(win)
    .bind(timestamp)
    .bind(blob)
    .execute(pool)
    .await?;
    Ok(())
}

fn process_seen_tft_match_score(
    match_info: &TftMatch,
    queue_scores: &mut HashMap<Queue, Score>,
    puuids: &HashSet<String>,
) -> Result<()> {
    // Group all TFT matches under one queue ID
    let game_score = queue_scores
        .entry(Queue::CONVERGENCE_TEAMFIGHT_TACTICS)
        .or_default();
    game_score.games += 1;

    let mut seen_placements = HashSet::new();
    for participant in match_info.info.participants.iter() {
        if puuids.contains(&participant.puuid) {
            let position = participant.placement;
            if seen_placements.insert(position) {
                if position == 1 {
                    game_score.wins += 1;
                } else if position <= 4 {
                    game_score.top_finishes += 1;
                } else {
                    game_score.bottom_finishes += 1;
                }
            }
        }
    }
    Ok(())
}

async fn update_tft_match_info(
    pool: &sqlx::SqlitePool,
    match_info: &TftMatch,
    queue_scores: &mut HashMap<Queue, Score>,
    puuids: &HashSet<String>,
    start_time: i64,
) -> Result<()> {
    let game_datetime = match_info.info.game_datetime;

    if game_datetime < start_time {
        info!(
            "TFT Match {} ended before start_time {}. Skipping.",
            match_info.metadata.match_id, start_time
        );
        return Ok(());
    }

    // Group all TFT matches under one queue ID
    let game_score = queue_scores
        .entry(Queue::CONVERGENCE_TEAMFIGHT_TACTICS)
        .or_default();
    game_score.games += 1;

    let mut is_win = false;
    let mut seen_placements = HashSet::new();
    for participant in match_info.info.participants.iter() {
        if puuids.contains(&participant.puuid) {
            let position = participant.placement;
            if seen_placements.insert(position) {
                if position == 1 {
                    is_win = true;
                    game_score.wins += 1;
                } else if position <= 4 {
                    game_score.top_finishes += 1;
                } else {
                    game_score.bottom_finishes += 1;
                }
            }
        }
    }

    let queue_id: i64 = u16::from(Queue::CONVERGENCE_TEAMFIGHT_TACTICS).into();
    save_tft_match(
        pool,
        &match_info.metadata.match_id,
        queue_id,
        is_win,
        game_datetime,
        match_info,
    )
    .await?;

    Ok(())
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

    // If we don't have two participants in the match, skip it
    if match_info.match_info.as_ref().is_none_or(|match_info| {
        match_info
            .info
            .participants
            .iter()
            .filter(|p| puuids.contains(&p.puuid))
            .count()
            < 2
    }) {
        return Ok(());
    }

    let game_score = queue_scores.entry(queue_id).or_default();

    let early_surrender = match &match_info.match_info {
        Some(info) => info
            .info
            .participants
            .first()
            .is_some_and(|p| p.game_ended_in_early_surrender),
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
                            } else if position <= 4 {
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

#[allow(clippy::too_many_arguments)]
async fn update_match_info(
    pool: &sqlx::SqlitePool,
    match_info: &Match,
    queue_scores: &mut HashMap<Queue, Score>,
    queue_ids: &HashMap<Queue, &str>,
    puuids: &HashSet<String>,
    pentakillers: &mut Vec<(i64, GameId)>,
    dom_earners: &mut Vec<(i64, GameId)>,
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

    let game_score = queue_scores.entry(match_info.info.queue_id).or_default();

    let early_surrender = match_info
        .info
        .participants
        .first()
        .is_some_and(|p| p.game_ended_in_early_surrender);

    if early_surrender {
        info!(
            "Match {} ended in early surrender. Skipping score update.",
            match_info.metadata.match_id
        );
        let queue_id: i64 = u16::from(match_info.info.queue_id).into();
        db::update_match(
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

    // Check if at least two discord users are present in the match, otherwise, early return OK(());
    if match_info
        .info
        .participants
        .iter()
        .filter(|p| puuids.contains(&p.puuid))
        .count()
        < 2
    {
        info!(
            "Match {} has less than 2 tracked PUUIDs. Skipping score update.",
            match_info.metadata.match_id
        );
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
                        } else if position <= 4 {
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

    db::update_match(
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
            .map(|p| {
                (
                    match_info.info.game_id,
                    GameId {
                        name: p.riot_id_game_name.as_ref().unwrap().to_string(),
                        tag: p.riot_id_tagline.as_ref().unwrap().to_string(),
                    },
                )
            }),
    );

    if match_info.info.queue_id != Queue::HOWLING_ABYSS_5V5_ARAM {
        pentakillers.extend(
            match_info
                .info
                .participants
                .iter()
                .filter(|p| puuids.contains(&p.puuid)) // Use borrowed puuids
                .filter(|p| p.penta_kills > 0)
                .map(|p| {
                    (
                        match_info.info.game_id,
                        GameId {
                            name: p.riot_id_game_name.as_ref().unwrap().to_string(),
                            tag: p.riot_id_tagline.as_ref().unwrap().to_string(),
                        },
                    )
                }),
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
    pentakillers: Vec<(i64, GameId)>,
    discord_auth: &Arc<serenity::http::Http>,
    config: &Config,
) -> Result<()> {
    if pentakillers.is_empty() {
        return Ok(());
    }

    let user_info = db::fetch_discord_usernames(&config.pool).await?;
    let mut pentakillers_by_discord_mention: Vec<_> = pentakillers
        .iter()
        .filter_map(|(_, summoner_id)| {
            user_info
                .get(&summoner_id.name().to_lowercase())
                .map(|discord_id| format!("<@{}>", discord_id))
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    pentakillers_by_discord_mention.sort();

    let mut pentakill_games: HashMap<i64, Vec<GameId>> = HashMap::new();
    for (game_id, riot_id) in pentakillers {
        pentakill_games.entry(game_id).or_default().push(riot_id);
    }

    let mention_names_formatted = format_names_list(&pentakillers_by_discord_mention);

    let mut pentakill_links = Vec::new();
    let unknown_id = GameId {
        name: String::from("player"),
        tag: String::from("NA1"),
    };
    for (game_id, riot_ids) in pentakill_games {
        let game_names_formatted =
            format_names_list(&riot_ids.iter().map(|id| id.name()).collect::<Vec<_>>());
        let riot_id = riot_ids.first().unwrap_or(&unknown_id);
        let url_encoded_riot_tag = urlencoding::encode(&riot_id.tag);
        let url_encoded_name = urlencoding::encode(&riot_id.name);
        pentakill_links.push(format!(
            "[{}'s pentakill game here](https://blitz.gg/lol/match/NA1/{}-{}/{})",
            game_names_formatted, url_encoded_name, url_encoded_riot_tag, game_id
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

async fn calculate_doms_info(
    dom_earners: Vec<(i64, GameId)>,
    config: &Config,
) -> Result<(String, Vec<String>)> {
    if dom_earners.is_empty() {
        return Ok((String::from(""), vec![]));
    }

    let user_info = db::fetch_discord_usernames(&config.pool).await?;
    let mut dom_earners_by_discord_mention: Vec<_> = dom_earners
        .iter()
        .filter_map(|(_, riot_id)| {
            user_info
                .get(&riot_id.name().to_lowercase())
                .map(|discord_id| format!("<@{}>", discord_id))
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    dom_earners_by_discord_mention.sort();

    let mut dom_games: HashMap<i64, Vec<GameId>> = HashMap::new();
    for (game_id, riot_id) in dom_earners {
        dom_games.entry(game_id).or_default().push(riot_id);
    }

    let mention_names_formatted = format_names_list(&dom_earners_by_discord_mention);

    let mut dom_links = Vec::new();
    let unknown_id = GameId {
        name: String::from("player"),
        tag: String::from("NA1"),
    };
    for (game_id, riot_ids) in dom_games {
        let game_names_formatted =
            format_names_list(&riot_ids.iter().map(|id| id.name()).collect::<Vec<_>>());
        let riot_id = riot_ids.first().unwrap_or(&unknown_id);
        let url_encoded_riot_tag = urlencoding::encode(&riot_id.tag);
        let url_encoded_name = urlencoding::encode(&riot_id.name);
        dom_links.push(format!(
            "[{}'s dom game here](https://blitz.gg/lol/match/NA1/{}-{}/{})",
            game_names_formatted, url_encoded_name, url_encoded_riot_tag, game_id
        ));
    }
    Ok((mention_names_formatted, dom_links))
}

async fn check_doms_info(
    dom_earners: Vec<(i64, GameId)>,
    discord_auth: &Arc<serenity::http::Http>,
    config: &Config,
) -> Result<()> {
    if dom_earners.is_empty() {
        return Ok(());
    }

    let (mention_names_formatted, dom_links) = calculate_doms_info(dom_earners, config).await?;

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
    use riven::consts::{PlatformRoute, Queue, Team as TeamId};
    use riven::models::match_v5::*;
    use sqlx::SqlitePool;
    use std::collections::{HashMap, HashSet};
    use std::fs;

    // Helper to set up an in-memory database for testing
    pub async fn setup_in_memory_db_pool() -> Result<SqlitePool> {
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
                 Puuid TEXT PRIMARY KEY,
                 TftPuuid TEXT,
                 Name TEXT NOT NULL UNIQUE
             );",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS MATCH (
                 Id TEXT PRIMARY KEY,
                 QUEUE_ID VARCHAR(255) NOT NULL, -- Storing as VARCHAR as in schema
                 Win BOOLEAN,
                 END_TIMESTAMP INTEGER, -- Storing as INTEGER (Unix millis)
                 MatchInfo BLOB
             );",
        )
        .execute(&pool)
        .await?;

        Ok(pool)
    }

    // Helper to create a basic Match struct for testing
    pub fn create_test_match(
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
                game_mode_mutators: None,
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
    pub fn create_test_participant(
        puuid: &str,
        riot_id: &str,
        team_id: TeamId,
        win: bool,
        kills: i32,
        deaths: i32,
        assists: i32,
        penta_kills: i32,
        placement: Option<i32>, // For Arena
        early_surrender: bool,
    ) -> Participant {
        // Construct the path relative to the Cargo manifest directory
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("test_data/participant_template.json");

        // Read the template file
        let template_json = fs::read_to_string(&path).unwrap_or_else(|e| {
            self::panic!("Failed to read participant template file {:?}: {}", path, e)
        });

        // Deserialize the template
        let mut participant: Participant = serde_json::from_str(&template_json)
            .unwrap_or_else(|e| self::panic!("Failed to deserialize participant template: {}", e));

        let game_name = GameId::try_from(riot_id).unwrap();

        // Override specific fields based on function arguments
        participant.puuid = puuid.to_string();
        participant.team_id = team_id;
        participant.win = win;
        participant.kills = kills;
        participant.deaths = deaths;
        participant.assists = assists;
        participant.penta_kills = penta_kills;
        participant.placement = placement;
        participant.game_ended_in_early_surrender = early_surrender;
        // Assume game_ended_in_surrender based on win/early_surrender for simplicity
        participant.game_ended_in_surrender = !win && !early_surrender;
        // Update teamEarlySurrendered based on the participant's team perspective
        participant.team_early_surrendered = early_surrender;
        participant.riot_id_game_name = Some(game_name.name.to_string());
        participant.riot_id_tagline = Some(game_name.tag.to_string());

        // You might want to update other fields based on win/loss/kills too, e.g., nexus kills
        participant.nexus_kills = if win { 1 } else { 0 };
        participant.nexus_lost = Some(if win { 0 } else { 1 });
        participant.nexus_takedowns = Some(if win { 1 } else { 0 });
        participant.double_kills = if kills >= 2 { 1 } else { 0 }; // Simple example
        participant.killing_sprees = if kills >= 3 { 1 } else { 0 }; // Simple example
        participant.largest_killing_spree = kills; // Simple example
        participant.largest_multi_kill = if penta_kills > 0 {
            5
        } else if kills >= 4 {
            4
        } else if kills >= 3 {
            3
        } else if kills >= 2 {
            2
        } else {
            1
        }; // Simple example

        participant
    }

    // Helper to create a basic Team struct
    pub fn create_test_team(team_id: TeamId, win: bool) -> riven::models::match_v5::Team {
        riven::models::match_v5::Team {
            bans: vec![],
            objectives: riven::models::match_v5::Objectives {
                baron: Objective {
                    first: false,
                    kills: 0,
                },
                champion: Objective {
                    first: win,
                    kills: 20,
                },
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
            feats: None,
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

    #[tokio::test]
    async fn test_update_match_info_standard_win() -> Result<()> {
        let pool = setup_in_memory_db_pool().await?; // Need pool for update_match call
        let mut queue_scores: HashMap<Queue, Score> = HashMap::new();
        let queue_ids: HashMap<Queue, &str> =
            HashMap::from([(Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO, "Solo")]);
        let puuids: HashSet<String> = HashSet::from(["puuid1".to_string(), "puuid2".to_string()]);
        let mut pentakillers = Vec::new();
        let mut dom_earners = Vec::new();
        let start_time = chrono::Local::now().timestamp_millis() - 1000 * 60 * 60;

        let p1 = create_test_participant(
            "puuid1",
            "Player1#NA1",
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
            "Player2#NA1",
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
        let puuids: HashSet<String> = HashSet::from([
            "puuid1".to_string(),
            "puuid2".to_string(),
            "puuid3".to_string(),
            "puuid4".to_string(),
        ]);
        let mut pentakillers = Vec::new();
        let mut dom_earners = Vec::new();
        let start_time = chrono::Local::now().timestamp_millis() - 1000 * 60 * 60;

        let p1 = create_test_participant(
            "puuid1",
            "Player1#NA1",
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
            "Player2#NA1",
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
            "Player1#NA1",
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
            "Player3#NA1",
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
            "Player1#NA1",
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
            "Player4#NA1",
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
        let puuids: HashSet<String> =
            HashSet::from(["puuid_penta".to_string(), "puuid_other".to_string()]);
        let mut pentakillers = Vec::new();
        let mut dom_earners = Vec::new();
        let start_time = chrono::Local::now().timestamp_millis() - 1000 * 60 * 60;

        let p1 = create_test_participant(
            "puuid_penta",
            "Penta Pete#NA3",
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
            "Feeder Fred#NA5",
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
        assert_eq!(pentakillers[0].1.name(), "Penta Pete");
        assert!(dom_earners.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_update_match_info_doms() -> Result<()> {
        let pool = setup_in_memory_db_pool().await?;
        let mut queue_scores: HashMap<Queue, Score> = HashMap::new();
        let queue_ids: HashMap<Queue, &str> =
            HashMap::from([(Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO, "Solo")]);
        let puuids: HashSet<String> =
            HashSet::from(["puuid_doms".to_string(), "puuid_other".to_string()]);
        let mut pentakillers = Vec::new();
        let mut dom_earners = Vec::new();
        let start_time = chrono::Local::now().timestamp_millis() - 1000 * 60 * 60;

        let p1 = create_test_participant(
            "puuid_doms",
            "Dominic#EUW1",
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
            "Average Andy#EUW3",
            TeamId::RED,
            false,
            5,
            5,
            4,
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
        assert_eq!(dom_earners[0].1.name(), "Dominic");
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
            "Arena Ace#NA2",
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
            "Arena Ally#NA4",
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
            "Rival 1#NA6",
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
            "Rival 2#NA7",
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
            "Bottom Feeder#NA8",
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
        assert_eq!(score.top_finishes, 0); // Both players were 1st, counts as one top finish
        assert_eq!(score.bottom_finishes, 0);

        let p6 = create_test_participant(
            "puuid_arena1",
            "Arena Ace#NA2",
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
            "Arena Ally#NA4",
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
        assert_eq!(score_after_2.top_finishes, 1); // Player 7 got 3rd place
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
            "Player1#NA1",
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
            "Player2#NA1",
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
            "Player1#NA1",
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
            "Player2#NA1",
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

        assert!(
            queue_scores
                .get(&Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO)
                .is_none()
        );
        assert!(pentakillers.is_empty());
        assert!(dom_earners.is_empty());
        Ok(())
    }

    #[test]
    fn test_format_names_list() {
        assert_eq!(format_names_list(&[]), "");
        assert_eq!(format_names_list(&["Alice".to_string()]), "Alice");
        assert_eq!(
            format_names_list(&["Alice".to_string(), "Bob".to_string()]),
            "Alice and Bob"
        );
        assert_eq!(
            format_names_list(&[
                "Alice".to_string(),
                "Bob".to_string(),
                "Charlie".to_string()
            ]),
            "Alice, Bob, and Charlie"
        );
    }
}
