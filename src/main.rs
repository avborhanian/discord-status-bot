use core::panic;
use std::collections::HashMap;
use std::collections::HashSet;
use std::env;
use std::sync::Arc;
use std::time::Duration;

use crate::models::MatchInfo;
use anyhow::Result;
use anyhow::{anyhow, Error, Ok};
use chrono::DateTime;
use chrono::Datelike;
use chrono::TimeZone;
use chrono::Timelike;
use dotenvy::dotenv;
use futures::future::FutureExt;
use futures::select;
use futures::stream;
use futures_util::StreamExt;
use http::Method;
#[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;
use models::Score;
use reqwest::header;
use riven::consts::{PlatformRoute, Queue, RegionalRoute};
use riven::models::match_v5::Match;
use riven::models::summoner_v4::Summoner;
use riven::RiotApi;
use serde_json::to_vec;
use serde_json::Value;
use serenity::async_trait;
use serenity::model::id::ChannelId;
use serenity::model::prelude::Interaction;
use serenity::model::prelude::InteractionResponseType;
use serenity::model::prelude::Member;
use serenity::prelude::*;
use tokio::sync::Mutex;
use tokio::task;
use tokio::time;
use tracing::{error, info};
use tracing_subscriber::filter;
use tracing_subscriber::prelude::*;

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
                                        .unwrap_or(&header::HeaderValue::from_str("2").unwrap())
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

fn get_channel_id(channel_name: &str) -> Result<u64> {
    match env::var(channel_name) {
        Result::Ok(s) => match s.parse::<u64>() {
            Result::Ok(id) => Ok(id),
            Err(_) => Result::Err(anyhow!(
                "The provided id '{}' was not a valid u64 number for {}.",
                s,
                channel_name
            )),
        },
        Err(_) => Result::Err(anyhow!(
            "Unable to find the channel '{}' in the environment.",
            channel_name
        )),
    }
}

struct Handler {
    users: Arc<RwLock<HashMap<u64, bool>>>,
    current_status: Arc<RwLock<String>>,
}

impl Handler {
    async fn remove_user(&self, member: &Member) {
        let mut users = self.users.write().await;
        users.remove(&member.user.id.0);
        if users.len() == 0 {
            let message = self.current_status.read().await.to_string();
            info!("Len is 0, going to update the status to {}", message);
            match update_discord_status(&message, None).await {
                Result::Ok(()) => {}
                Err(e) => error!("Error while updating: {}", e),
            }
        } else {
            info!("Users len is {}, not updating.", users.len());
        }
    }
}

#[async_trait]
impl EventHandler for Handler {
    async fn ready(&self, ctx: serenity::prelude::Context, _data: serenity::model::prelude::Ready) {
        info!("Ready event received");
        if let Result::Ok(channel) = ctx
            .http
            .get_channel(get_channel_id("VOICE_CHANNEL_ID").unwrap())
            .await
        {
            info!("Games channel passed");
            if let serenity::model::prelude::Channel::Guild(guild_channel) = channel {
                info!("It's a guild channel");
                if let Result::Ok(members) = guild_channel.members(ctx.cache).await {
                    info!("We found members");
                    let mut users = self.users.write().await;
                    for member in members {
                        users.insert(*member.user.id.as_u64(), false);
                    }
                }
                info!(
                    "Users was updated to have a length of {}",
                    self.users.read().await.len()
                );
            }
        }

        match serenity::model::id::GuildId::set_application_commands(
            &serenity::model::id::GuildId(get_channel_id("DISCORD_GUILD_ID").unwrap()),
            &ctx.http,
            |commands| {
                commands
                    .create_application_command(|command| commands::groups::register(command))
                    .create_application_command(|command| {
                        commands::globetrotters::register(command)
                    })
                // TODO: Re-enable once register fixed.
                // .create_application_command(|command| commands::register::register(command))
            },
        )
        .await
        {
            Result::Ok(_) => {}
            Result::Err(e) => error!("Ran into error while trying to set up commands: {}", e),
        };
    }

    async fn interaction_create(&self, ctx: serenity::prelude::Context, interaction: Interaction) {
        if let Interaction::ApplicationCommand(command) = interaction {
            info!(
                "Received command interaction: {}",
                command.data.name.as_str()
            );

            let content = match command.data.name.as_str() {
                "groups" => commands::groups::run(&ctx, &command.data.options).await,
                "globetrotters" => commands::globetrotters::run(&ctx, &command.data.options),
                // TODO: Re-enable once register fixed.
                // "register" => commands::register::run(&ctx, &command.data.options).await,
                _ => "not implemented :(".to_string(),
            };

            if command.data.name.as_str() == "register" {
                info!("Register response: {}", content);
                return;
            }

            if let Err(why) = command
                .create_interaction_response(&ctx.http, |response| {
                    response
                        .kind(InteractionResponseType::ChannelMessageWithSource)
                        .interaction_response_data(|message| message.content(content))
                })
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
        let voice_channel_id = ChannelId(get_channel_id("VOICE_CHANNEL_ID").unwrap());
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
                    users.insert(member.user.id.0, false);
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
                    users.insert(member.user.id.0, false);
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
                return Err(anyhow!("Unable to sub day"));
            }
        }
    }
    datetime = chrono::Local
        .with_ymd_and_hms(
            datetime.date_naive().year(),
            datetime.date_naive().month(),
            datetime.date_naive().day(),
            6,
            0,
            0,
        )
        .unwrap();
    Result::Ok(datetime.timestamp())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().expect(".env file not found");
    std::panic::set_hook(Box::new(|i| {
        error!("Panic'd: {}", i);
    }));
    let riot_api_key: &str = &env::var("RIOT_API_TOKEN")?;
    let riot_api = RiotApi::new(riot_api_key);
    let file_appender = tracing_appender::rolling::daily(
        if cfg!(target_os = "linux") {
            "/var/logs/discord"
        } else {
            "./"
        },
        "server.log",
    );
    let (non_blocking_appender, _guard) = tracing_appender::non_blocking(file_appender);
    let console_layer = console_subscriber::ConsoleLayer::builder()
        // set the address the server is bound to
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

    // Login with a bot token from the environment
    let token = env::var("DISCORD_BOT_TOKEN").expect("token");
    let intents = GatewayIntents::non_privileged()
        | GatewayIntents::GUILDS
        | GatewayIntents::GUILD_MEMBERS
        | GatewayIntents::GUILD_MESSAGES;
    let handler = Handler {
        users: active_users.clone(),
        current_status: current_status.clone(),
    };
    let mut client = Client::builder(&token, intents)
        .event_handler(handler)
        .await
        .expect("Error creating client");

    let http_client = reqwest::Client::builder()
        .use_rustls_tls()
        .connection_verbose(true)
        .build()?;
    let discord_client = Arc::new(
        serenity::http::HttpBuilder::new(&token)
            .ratelimiter_disabled(true)
            .client(http_client)
            .build(),
    );
    let discord_events = tokio::spawn(async move {
        match client.start().await {
            Result::Ok(s) => Result::Ok(s),
            Result::Err(e) => Result::Err(anyhow!(e)),
        }
    });

    let forever: task::JoinHandle<std::prelude::v1::Result<(), Error>> = task::spawn(async move {
        let discord_client = discord_client.clone();
        let current_status = current_status.clone();
        let mut start_time = 0;
        let mut current_date = 0;
        let mut interval = time::interval(Duration::from_secs(5));
        let summoners = fetch_summoners()?;
        loop {
            let active_summoners = summoners.iter().map(|s| s.1).collect::<HashSet<_>>();
            let puuids = fetch_puuids(&active_summoners)?;
            for puuid in &puuids {
                interval.tick().await;
                let matches_fetch: Result<Vec<String>> = riot_api!(
                    riot_api
                        .match_v5()
                        .get_match_ids_by_puuid(
                            RegionalRoute::AMERICAS,
                            puuid,
                            None,
                            None,
                            None,
                            Some(start_time),
                            None,
                            None,
                        )
                        .await
                );
                let new_start_time = get_start_time()?;
                match (current_date < new_start_time, matches_fetch) {
                    (true, _) => {
                        info!("It's a new day, time to scan histories at least once. Current day was {}, now it's {}", current_date, new_start_time);
                        match check_match_history(&riot_api, &discord_client, &current_status).await
                        {
                            Result::Ok(()) => {}
                            Result::Err(e) => error!("{}", e),
                        }
                        start_time = chrono::Local::now().timestamp();
                        info!("Also setting start time to {}", start_time);
                        current_date = new_start_time;
                    }
                    (_, Result::Ok(match_list)) if !match_list.is_empty() => {
                        info!("Some matches were found, scan all histories now. Current time set to {}. Current date is {}", start_time, current_date);
                        match check_match_history(&riot_api, &discord_client, &current_status).await
                        {
                            Result::Ok(()) => {}
                            Result::Err(e) => error!("{}", e),
                        }
                        start_time = chrono::Local::now().timestamp();
                        info!("Also setting start time to {}", start_time);
                    }
                    (false, Result::Ok(_)) => {}
                    (_, Result::Err(e)) => error!("Hit an error while fetching: {}", e),
                }
            }
        }
    });

    match select! {
        a_res = discord_events.fuse() => a_res,
        b_res = forever.fuse() => b_res
    } {
        Result::Ok(s) => info!("Finished? {:?}", s),
        Result::Err(e) => error!("Some error occured here. {:?}", e),
    };
    Ok(())
}

fn update_match(
    match_id: &String,
    queue_id: i64,
    is_win: bool,
    end_timestamp: i64,
    match_info: &Match,
) -> Result<()> {
    let connection = rusqlite::Connection::open("sqlite.db")?;
    let mut statement = connection
        .prepare("INSERT OR IGNORE INTO match (id, queue_id, win, end_timestamp, MatchInfo) VALUES (?1, ?2, ?3, ?4, ?5);")?;
    statement.execute([
        match_id,
        &queue_id.to_string(),
        &is_win.to_string(),
        &end_timestamp.to_string(),
        &serde_json::to_string(match_info)?,
    ])?;
    Ok(())
}

fn fetch_discord_usernames() -> Result<HashMap<String, String>> {
    let mut summoner_map: HashMap<String, String> = HashMap::new();
    let connection = rusqlite::Connection::open("sqlite.db")?;
    let mut statement = connection.prepare("SELECT DiscordId, LOWER(summonerName) FROM User")?;
    statement
        .query_map([], |row| {
            let discord_id: String = row.get(0).unwrap();
            let summoner = row.get(1).unwrap();
            Result::Ok((discord_id, summoner))
        })?
        .for_each(|result| {
            let (discord_id, summoner) = result.unwrap();
            summoner_map.insert(summoner, discord_id);
        });
    Ok(summoner_map)
}

fn fetch_puuids(summoners: &HashSet<&String>) -> Result<HashSet<String>> {
    let mut puuids = HashSet::new();
    let connection = rusqlite::Connection::open("sqlite.db")?;
    let mut statement = connection.prepare(&format!(
        "SELECT puuid FROM summoner WHERE name in ({})",
        summoners
            .iter()
            .map(|name| format!("'{name}'"))
            .collect::<Vec<_>>()
            .join(",")
    ))?;
    statement
        .query_map([], |row| row.get(0))?
        .for_each(|result| {
            puuids.insert(result.unwrap());
        });
    Ok(puuids)
}

fn fetch_summoners() -> Result<HashMap<u64, String>> {
    let connection = rusqlite::Connection::open("sqlite.db")?;
    let mut statement = connection.prepare("SELECT DiscordId, SummonerName FROM USER;")?;
    let query = statement.query_map([], |row| {
        Result::Ok((
            row.get::<usize, String>(0).unwrap().parse::<u64>().unwrap(),
            row.get(1).unwrap(),
        ))
    })?;
    Ok(query
        .map(std::result::Result::unwrap)
        .collect::<HashMap<u64, String>>())
}

async fn fetch_seen_events(
    matches: &Arc<Mutex<HashMap<String, u8>>>,
) -> Result<HashMap<String, MatchInfo>> {
    let match_id_string = {
        matches
            .lock()
            .await
            .clone()
            .into_keys()
            .map(|id| format!("'{id}'"))
            .collect::<Vec<_>>()
            .join(",")
    };
    let connection = rusqlite::Connection::open("sqlite.db")?;
    let mut statement = connection.prepare(&format!(
        "SELECT id, queue_id, win, IFNULL(end_timestamp, 0), MatchInfo FROM match WHERE id IN ({match_id_string});"
    ))?;
    let mut seen_matches: HashMap<String, MatchInfo> = HashMap::new();
    statement
        .query_map([], |row| {
            Result::Ok(MatchInfo {
                id: row.get(0).unwrap(),
                queue_id: row.get(1).unwrap(),
                win: row.get::<usize, String>(2).unwrap() == "true",
                end_timestamp: chrono::NaiveDateTime::from_timestamp_millis(row.get(3).unwrap())
                    .unwrap(),
                match_info: serde_json::from_str(&row.get::<usize, String>(4).unwrap()).unwrap(),
            })
        })?
        .for_each(|result| {
            let match_info = result.unwrap();
            seen_matches
                .entry(match_info.id.clone())
                .or_insert(match_info);
        });
    Ok(seen_matches)
}

async fn check_match_history(
    riot_api: &RiotApi,
    discord_client: &Arc<serenity::http::Http>,
    current_status: &Arc<RwLock<String>>,
) -> Result<()> {
    let queue_ids: HashMap<Queue, &str> = HashMap::from([
        (Queue::SUMMONERS_RIFT_5V5_DRAFT_PICK, "Draft"),
        (Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO, "Solo"),
        (Queue::SUMMONERS_RIFT_5V5_BLIND_PICK, "Blind"),
        (Queue::SUMMONERS_RIFT_5V5_RANKED_FLEX, "Flex"),
        (Queue::HOWLING_ABYSS_5V5_ARAM, "ARAM"),
        (Queue::SUMMONERS_RIFT_CLASH, "Clash"),
        (Queue::ARENA_2V2V2V2_CHERRY, "Arena"),
    ]);
    let mut queue_scores: HashMap<Queue, Score> = HashMap::new();
    let start_time = get_start_time()?;

    let summoner_info = fetch_summoners()?;
    let summoners = summoner_info.values().collect::<HashSet<_>>();
    let puuids = fetch_puuids(&summoners)?;

    let matches: Arc<Mutex<HashMap<String, u8>>> = Arc::new(Mutex::new(HashMap::new()));

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
        .buffer_unordered(5);
    let matches_list: Vec<_> = matches_requests.collect().await;

    let iter = matches_list.iter();
    for result in iter {
        match result {
            Result::Ok(match_ids) => {
                let mut lock = matches.lock().await;
                for match_id in match_ids {
                    *lock.entry(match_id.to_string()).or_insert(0) += 1;
                }
            }
            Result::Err(e) => {
                return Err(anyhow!("Encountered error: {:?}", e));
            }
        }
    }

    let seen_matches = fetch_seen_events(&matches).await?;
    let mut pentakillers = Vec::new();
    let mut dom_earners = Vec::new();

    let mut match_list = {
        let list = matches.lock().await;
        let result = list.keys().cloned().collect::<Vec<_>>();
        result
    };
    match_list.sort();
    for match_id in match_list {
        // Only one of our players in this game, skip the score.
        if matches.lock().await.get(&match_id).unwrap() == &1 {
            continue;
        }
        if seen_matches.contains_key(&match_id) {
            if let Some(match_info) = seen_matches.get(&match_id) {
                let queue_id = Queue::from(match_info.queue_id.parse::<u16>()?);
                let game_score = queue_scores.entry(queue_id).or_insert(Score {
                    wins: 0,
                    games: 0,
                    warmup: 0,
                    top_finishes: 0,
                });
                if let Some(info) = &match_info.match_info {
                    if let Some(participant) = info.info.participants.first() {
                        if participant.game_ended_in_early_surrender {
                            continue;
                        }
                    }
                    if info.info.queue_id == Queue::ARENA_2V2V2V2_CHERRY {
                        game_score.games += 1;
                        // We instead need to do top finishes/wins/games.
                        let mut seen_uuids = HashSet::new();
                        for participant in info.info.participants.iter() {
                            if puuids.contains(&participant.puuid) {
                                let position = participant.placement.unwrap();
                                if seen_uuids.insert(position) {
                                    if position <= 4 {
                                        game_score.top_finishes += 1;
                                    }
                                    if position == 1 {
                                        game_score.wins += 1;
                                    }
                                }
                            }
                        }
                    }
                }
                game_score.games += 1;
                if match_info.win {
                    game_score.wins += 1;
                } else if game_score.warmup < 2
                    && game_score.wins < 1
                    && queue_id != Queue::SUMMONERS_RIFT_CLASH
                {
                    game_score.warmup += 1;
                    game_score.games -= 1;
                }
                continue;
            }
        }
        let response = riot_api!(
            riot_api
                .match_v5()
                .get_match(RegionalRoute::AMERICAS, &match_id)
                .await
        )?;
        let Some(match_info) = response else {
            error!("Unable to find match id {}", match_id);
            continue;
        };

        update_match_info(
            &match_info,
            &mut queue_scores,
            &queue_ids,
            &puuids,
            &mut pentakillers,
            &mut dom_earners,
            start_time,
        )?;
    }

    check_pentakill_info(pentakillers, discord_client).await?;
    check_doms_info(dom_earners, discord_client).await?;

    let mut queue_scores_msgs = queue_scores
        .iter()
        .filter(|(_, score)| score.games > 0 || score.warmup > 0)
        .map(|(queue_id, score)| {
            let game_mode = queue_ids.get(queue_id).unwrap();
            if *queue_id == Queue::ARENA_2V2V2V2_CHERRY {
                return format!(
                    "{game_mode}:\u{2006}{}-{}-{}",
                    score.wins,
                    score.top_finishes - score.wins,
                    score.games - score.top_finishes
                );
            } else if score.warmup > 0
                && score.games == 0
                && *queue_id != Queue::SUMMONERS_RIFT_CLASH
            {
                return format!("{game_mode}: 🏃");
            }
            format!(
                "{game_mode}:\u{2006}{}\u{2006}-\u{2006}{}",
                score.wins,
                score.games - score.wins
            )
        })
        .collect::<Vec<_>>();
    queue_scores_msgs.sort();
    let mut results = queue_scores_msgs.join("\u{2006}|\u{2006}");
    if queue_scores.is_empty() {
        results = String::from("No group rift games yet!");
    } else {
        // We're too long, so need to do some tricks to shorten the length.
        if results.chars().collect::<Vec<char>>().len() > 40 {
            queue_scores_msgs = queue_scores
                .iter()
                .map(|(queue_id, score)| {
                    let game_mode = queue_ids.get(queue_id).unwrap();
                    if score.warmup > 0
                        && score.games == 0
                        && *queue_id != Queue::SUMMONERS_RIFT_CLASH
                    {
                        return format!("{game_mode}: 🏃");
                    }
                    format!(
                        "{game_mode}:\u{2006}{}\u{2006}-\u{2006}{}",
                        score.wins,
                        score.games - score.wins
                    )
                })
                .collect::<Vec<_>>();
            queue_scores_msgs.sort();
            results = queue_scores_msgs.join("\u{2006}|\u{2006}");
        }
        // We're still too long, so need to do some tricks to shorten the length.
        if results.chars().collect::<Vec<char>>().len() > 40 {
            queue_scores_msgs = queue_scores
                .iter()
                .map(|(queue_id, score)| {
                    let game_mode = queue_ids
                        .get(queue_id)
                        .unwrap()
                        .chars()
                        .next()
                        .unwrap()
                        .to_uppercase();
                    if score.warmup > 0
                        && score.games == 0
                        && *queue_id != Queue::SUMMONERS_RIFT_CLASH
                    {
                        return format!("{game_mode}: 🏃");
                    }
                    format!(
                        "{game_mode}:\u{2006}{}\u{2006}-\u{2006}{}",
                        score.wins,
                        score.games - score.wins
                    )
                })
                .collect::<Vec<_>>();
            queue_scores_msgs.sort();
            results = queue_scores_msgs.join("\u{2006}|\u{2006}");
        }
    }

    update_discord_status(&results, Some(current_status)).await
}

fn update_match_info(
    match_info: &Match,
    queue_scores: &mut HashMap<Queue, Score>,
    queue_ids: &HashMap<Queue, &str>,
    puuids: &HashSet<String>,
    pentakillers: &mut Vec<(i64, String)>,
    dom_earners: &mut Vec<(i64, String)>,
    start_time: i64,
) -> Result<()> {
    let Some(game_end_timestamp) = match_info.info.game_end_timestamp else {
        info!("Found the match, but can't get a end timestamp. So continuing.");
        return Result::Ok(());
    };
    if game_end_timestamp < start_time {
        return Result::Ok(());
    }

    if !queue_ids.contains_key(&match_info.info.queue_id) {
        return Result::Ok(());
    }

    let game_score = queue_scores
        .entry(match_info.info.queue_id)
        .or_insert(Score {
            wins: 0,
            games: 0,
            warmup: 0,
            top_finishes: 0,
        });

    let early_surrender = match_info
        .info
        .participants
        .first()
        .unwrap()
        .game_ended_in_early_surrender;
    if !early_surrender {
        game_score.games += 1;
    }

    let queue_id: i64 = {
        let id: u16 = match_info.info.queue_id.into();
        id.into()
    };

    let team_id = match_info
        .info
        .participants
        .iter()
        .find(|p| puuids.clone().contains(&p.puuid))
        .unwrap()
        .team_id;

    if match_info.info.queue_id == Queue::ARENA_2V2V2V2_CHERRY {
        // We instead need to do top finishes/wins/games.
        let mut seen_uuids = HashSet::new();
        for participant in match_info.info.participants.iter() {
            if puuids.contains(&participant.puuid) {
                let position = participant.placement.unwrap();
                if seen_uuids.insert(position) {
                    if position <= 4 {
                        game_score.top_finishes += 1;
                    }
                    if position == 1 {
                        game_score.wins += 1;
                    }
                }
            }
        }
        update_match(
            &match_info.metadata.match_id,
            queue_id,
            false,
            game_end_timestamp,
            match_info,
        )?;
    } else if match_info
        .info
        .teams
        .iter()
        .find(|t| t.team_id == team_id)
        .unwrap()
        .win
    {
        if !early_surrender {
            game_score.wins += 1;
        }
        update_match(
            &match_info.metadata.match_id,
            queue_id,
            true,
            game_end_timestamp,
            match_info,
        )?;
    } else {
        if game_score.warmup < 2 && game_score.wins < 1 && !early_surrender {
            game_score.warmup += 1;
            game_score.games -= 1;
        };
        update_match(
            &match_info.metadata.match_id,
            queue_id,
            false,
            game_end_timestamp,
            match_info,
        )?;
    };

    dom_earners.extend(
        match_info
            .info
            .participants
            .iter()
            .filter(|p| puuids.clone().contains(&p.puuid))
            .filter(|p| p.kills == 5 && p.assists == 5 && p.deaths == 5)
            .map(|p| (match_info.info.game_id, p.summoner_name.clone())),
    );

    if match_info.info.queue_id != Queue::HOWLING_ABYSS_5V5_ARAM {
        pentakillers.extend(
            match_info
                .info
                .participants
                .iter()
                .filter(|p| puuids.clone().contains(&p.puuid))
                .filter(|p| p.penta_kills > 0)
                .map(|p| (match_info.info.game_id, p.summoner_name.clone())),
        );
    }
    Ok(())
}

async fn check_pentakill_info(
    mut pentakillers: Vec<(i64, String)>,
    discord_auth: &Arc<serenity::http::Http>,
) -> Result<()> {
    if !pentakillers.is_empty() {
        let user_info = fetch_discord_usernames()?;
        let mut pentakillers_by_name: Vec<_> = pentakillers
            .iter()
            .map(|s| format!("<@{}>", *user_info.get(&s.1.to_lowercase()).unwrap()))
            .collect::<HashSet<_>>()
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        pentakillers_by_name.sort();
        pentakillers.sort();

        let mut pentakill_games: HashMap<i64, Vec<String>> = HashMap::new();
        for (game_id, name) in pentakillers {
            let game_info = pentakill_games.entry(game_id).or_default();
            game_info.push(name);
        }

        let len = pentakillers_by_name.len();
        let names: String = match len {
            1 => pentakillers_by_name.first().unwrap().to_string(),
            2 => format!(
                "{} and {}",
                pentakillers_by_name.first().unwrap(),
                pentakillers_by_name.last().unwrap()
            ),
            _ => format!(
                "{}, and {}",
                pentakillers_by_name[0..len - 2].join(", "),
                pentakillers_by_name.last().unwrap()
            ),
        };

        let mut pentakill_links = Vec::new();

        for (game_id, names) in pentakill_games {
            let formatted_names: String = match len {
                1 => names.first().unwrap().to_string(),
                2 => format!("{} and {}", names.first().unwrap(), names.last().unwrap()),
                _ => format!(
                    "{}, and {}",
                    names[0..len - 2].join(", "),
                    names.last().unwrap()
                ),
            };
            pentakill_links.push(format!(
                "[{}'s pentakill game here](https://blitz.gg/lol/match/na1/{}/{})",
                formatted_names,
                names.first().unwrap().replace(' ', "%20"),
                game_id
            ));
        }

        update_highlight_reel(
            &format!(
                "Make sure to congratulate {} on their pentakill{}!\n\n{}",
                names,
                if len > 1 { "s" } else { "" },
                pentakill_links.join("\n")
            ),
            discord_auth,
        )
        .await?;
    }

    Ok(())
}

async fn check_doms_info(
    mut dom_earners: Vec<(i64, String)>,
    discord_auth: &Arc<serenity::http::Http>,
) -> Result<()> {
    if !dom_earners.is_empty() {
        let user_info = fetch_discord_usernames()?;
        let mut dom_earners_by_name: Vec<_> = dom_earners
            .iter()
            .map(|s| format!("<@{}>", *user_info.get(&s.1.to_lowercase()).unwrap()))
            .collect::<HashSet<_>>()
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        dom_earners_by_name.sort();
        dom_earners.sort();

        let mut dom_games: HashMap<i64, Vec<String>> = HashMap::new();
        for (game_id, name) in dom_earners {
            let game_info = dom_games.entry(game_id).or_default();
            game_info.push(name);
        }

        let len = dom_earners_by_name.len();
        let names: String = match len {
            1 => dom_earners_by_name.first().unwrap().to_string(),
            2 => format!(
                "{} and {}",
                dom_earners_by_name.first().unwrap(),
                dom_earners_by_name.last().unwrap()
            ),
            _ => format!(
                "{}, and {}",
                dom_earners_by_name[0..len - 2].join(", "),
                dom_earners_by_name.last().unwrap()
            ),
        };

        let mut dom_links = Vec::new();

        for (game_id, names) in dom_games {
            let formatted_names: String = match len {
                1 => names.first().unwrap().to_string(),
                2 => format!("{} and {}", names.first().unwrap(), names.last().unwrap()),
                _ => format!(
                    "{}, and {}",
                    names[0..len - 2].join(", "),
                    names.last().unwrap()
                ),
            };
            dom_links.push(format!(
                "[{}'s dom game here](https://blitz.gg/lol/match/na1/{}/{})",
                formatted_names,
                names.first().unwrap().replace(' ', "%20"),
                game_id
            ));
        }

        update_highlight_reel("🚨 SOMEONE JUST GOT DOMS 🚨", discord_auth).await?;
        update_highlight_reel(
            "https://tenor.com/view/dominos-dominos-pizza-pizza-dominos-guy-gif-7521435",
            discord_auth,
        )
        .await?;
        update_highlight_reel(
            &format!(
                "{} just earned doms! Here's their games:\n{}",
                names,
                dom_links.join("\n")
            ),
            discord_auth,
        )
        .await?;
    }

    Ok(())
}

async fn update_discord_status(
    results: &str,
    current_status: Option<&Arc<RwLock<String>>>,
) -> Result<()> {
    if let Some(current_status) = current_status {
        let mut s = current_status.write().await;
        *s = results.to_string();
        info!("Current status has been updated to '{}'", results);
    }
    let mut content = serde_json::Map::new();
    content.insert(String::from("status"), results.to_string().into());
    let map: Value = content.into();
    let owned_map = to_vec(&map.clone())?;
    let create_message = serenity::http::routing::RouteInfo::CreateMessage {
        channel_id: get_channel_id("VOICE_CHANNEL_ID")?,
    };
    let mut request_builder = serenity::http::request::RequestBuilder::new(create_message);
    request_builder.body(Some(&owned_map));
    let mut request = request_builder.build();
    let client = reqwest::Client::builder()
        .use_rustls_tls()
        .connection_verbose(true)
        .build()?;
    let token = format!("Bot {}", env::var("DISCORD_BOT_TOKEN")?);
    let mut true_reqwest = request.build(&client, &token, None).await?.build()?;
    true_reqwest.headers_mut().append(
        header::HeaderName::from_lowercase(b"x-super-properties")?,
        header::HeaderValue::from_static(
            "eyJvcyI6IldpbmRvd3MiLCJicm93c2VyIjoiQ2hyb21lIiwiZGV2aWNlIjoiIiwic3lzdGVtX2xvY2FsZSI6ImVuLVVTIiwiYnJvd3Nlcl91c2VyX2FnZW50IjoiTW96aWxsYS81LjAgKFdpbmRvd3MgTlQgMTAuMDsgV2luNjQ7IHg2NCkgQXBwbGVXZWJLaXQvNTM3LjM2IChLSFRNTCwgbGlrZSBHZWNrbykgQ2hyb21lLzExNi4wLjAuMCBTYWZhcmkvNTM3LjM2IiwiYnJvd3Nlcl92ZXJzaW9uIjoiMTE2LjAuMC4wIiwib3NfdmVyc2lvbiI6IjEwIiwicmVmZXJyZXIiOiJodHRwczovL3d3dy5nb29nbGUuY29tLyIsInJlZmVycmluZ19kb21haW4iOiJ3d3cuZ29vZ2xlLmNvbSIsInNlYXJjaF9lbmdpbmUiOiJnb29nbGUiLCJyZWZlcnJlcl9jdXJyZW50IjoiIiwicmVmZXJyaW5nX2RvbWFpbl9jdXJyZW50IjoiIiwicmVsZWFzZV9jaGFubmVsIjoic3RhYmxlIiwiY2xpZW50X2J1aWxkX251bWJlciI6MjI2MjIwLCJjbGllbnRfZXZlbnRfc291cmNlIjpudWxsfQ==",
        ),
    );
    true_reqwest.url_mut().set_path(&format!(
        "api/v10/channels/{}/voice-status",
        get_channel_id("VOICE_CHANNEL_ID")?,
    ));
    *true_reqwest.method_mut() = Method::PUT;
    let response = client.execute(true_reqwest).await;
    match response {
        Result::Ok(_) => {}
        Result::Err(e) => {
            error!("Ran into an error while trying to update the status: {}", e);
        }
    }
    Ok(())
}

async fn update_highlight_reel(
    results: &str,
    discord_auth: &Arc<serenity::http::Http>,
) -> Result<()> {
    let mut content = serde_json::Map::new();
    content.insert(
        String::from("content"),
        serde_json::Value::String(results.to_string()),
    );
    match discord_auth
        .send_message(get_channel_id("UPDATES_CHANNEL_ID")?, &content.into())
        .await
    {
        Result::Ok(message) => info!("Message id is {}", message.id),
        Result::Err(e) => error!("Ran into an error while sending. Error: {}", e),
    };

    Ok(())
}

#[allow(dead_code)]
fn calculate_match_performance(match_info: &Match, discord_puuids: &HashSet<String>) -> Result<()> {
    let discord_participants = match_info
        .info
        .participants
        .iter()
        .filter(|p| discord_puuids.contains(&p.puuid))
        .collect::<Vec<_>>();

    let log_models: log_models::LogModels =
        serde_json::from_str(include_str!("log_models.json")).expect("Valid format");
    for participant in discord_participants {
        println!(
            "Looking for role {} and lane {}",
            participant.role, participant.lane,
        );
        let log_model = log_models
            .models
            .iter()
            .find(|m| m.role == participant.role && m.lane == participant.lane)
            .unwrap();
        let mut score: f64 = log_model.intercept;
        for param in &log_model.params {
            score += param.coefficient
                * f64::from(match param.name.as_str() {
                    "assists" => participant.assists,
                    "goldEarned" => participant.gold_earned,
                    "totalMinionsKilled" => participant.total_minions_killed,
                    "wardsKilled" => participant.wards_killed,
                    "largestKillingSpree" => participant.largest_killing_spree,
                    "quadraKills" => participant.quadra_kills,
                    "timeCCingOthers" => participant.time_c_cing_others,
                    "visionScore" => participant.vision_score,
                    "totalEnemyJungleMinionsKilled" => participant
                        .total_enemy_jungle_minions_killed
                        .unwrap_or_default(),
                    "killingSprees" => participant.killing_sprees,
                    "largestMultiKill" => participant.largest_multi_kill,
                    "visionWardsBoughtInGame" => participant.vision_wards_bought_in_game,
                    "totalAllyJungleMinionsKilled" => participant
                        .total_ally_jungle_minions_killed
                        .unwrap_or_default(),
                    "bountyLevel" => participant.bounty_level,
                    "dragonKills" => participant.dragon_kills,
                    "kills" => participant.kills,
                    "baronKills" => participant.baron_kills,
                    "tripleKills" => participant.triple_kills,
                    "totalUnitsHealed" => participant.total_units_healed,
                    "deaths" => participant.deaths,
                    "objectivesStolen" => participant.objectives_stolen,
                    "goldSpent" => participant.gold_spent,
                    "consumablesPurchased" => participant.consumables_purchased,
                    "objectivesStolenAssists" => participant.objectives_stolen_assists,
                    "doubleKills" => participant.double_kills,
                    "wardsPlaced" => participant.wards_placed,
                    "turretTakedowns" => participant.turret_takedowns.unwrap_or_default(),
                    "pentaKills" => participant.penta_kills,
                    "firstBloodAssist" => i32::from(participant.first_blood_assist),
                    "turretKills" => participant.turret_kills,
                    "totalTimeSpentDead" => participant.total_time_spent_dead,
                    "detectorWardsPlaced" => participant.detector_wards_placed,
                    "totalDamageShieldedOnTeammates" => {
                        participant.total_damage_shielded_on_teammates
                    }
                    _ => {
                        return Err(anyhow!("Unhandled param: {}", param.name));
                    }
                })
                / match param.name.as_str() {
                    "firstBloodAssist" => 1.0,
                    _ => {
                        (match &participant.challenges {
                            Some(challenge) => challenge.game_length.unwrap_or(30.0),
                            None => 30.0,
                        }) / 60.0
                    }
                };
        }
        let probability = 1. / (1. + (-score).exp());
        println!(
            "For participant {}, probability was {}",
            participant.summoner_name, probability
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use riven::models::match_v5::Match;

    use crate::calculate_match_performance;

    #[test]
    fn does_calculate_match_performance() {
        let match_: Match = serde_json::from_str(include_str!("../tests/NA1_4802938104.json"))
            .expect("Valid Format");
        let discord_puuids = HashSet::from([String::from(
            "N0druvX0j969WpG6Py5LBI2OctCB-ZCDhqAR0fjjBn8yzjWjaJFmJEjZQh1X09evWBO9JDpBe3JOEg",
        )]);
        calculate_match_performance(&match_, &discord_puuids).unwrap();
    }
}
