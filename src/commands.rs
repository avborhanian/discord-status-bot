use anyhow::{anyhow, Result};

fn f64_to_usize(value: f64) -> Result<usize> {
    let result = value.round() as usize;
    if (result as f64) != value {
        return Result::Err(anyhow!("The provided value was not a whole number."));
    }
    Result::Ok(result)
}

pub mod globetrotters {
    use anyhow::{anyhow, Result};
    use serenity::all::CommandDataOptionValue;
    use serenity::all::CreateCommandOption;
    use serenity::all::CreateInteractionResponseMessage;
    use serenity::builder;
    use serenity::model::application::CommandDataOption;
    use serenity::model::application::CommandOptionType;
    use std::collections::HashMap;
    use tracing::error;

    pub fn register() -> builder::CreateCommand {
        builder::CreateCommand::new("globetrotters")
            .description("Returns a list of champions required to complete a challenge.")
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::Integer,
                    "challenge",
                    "Which challenge/region you want to complete",
                )
                .add_int_choice("5 Under 5' - Bandle City", 303_501)
                .add_int_choice("All Hands on Deck - Bilgewater", 303_502)
                .add_int_choice("FOR DEMACIA - Demacia", 303_503)
                .add_int_choice("Ice, Ice, Baby - the Freljord", 303_504)
                .add_int_choice("Everybody was Wuju Fighting - Ionia", 303_505)
                .add_int_choice("Elemental, My Dear Watson - Ixtal", 303_506)
                .add_int_choice("Strength Above All - Noxus", 303_507)
                .add_int_choice("Calculated - Piltover", 303_508)
                .add_int_choice("Spooky Scary Skeletons - the Shadow Isles", 303_509)
                .add_int_choice("The Sun Disc Never Sets - Shurima", 303_510)
                .add_int_choice("Peak Performance - Targon", 303_511)
                .add_int_choice("(Inhuman Screeching Sounds) - the Void", 303_512)
                .add_int_choice("Chemtech Comrades - Zaun", 303_513)
                .required(true),
            )
    }

    pub fn run(
        _ctx: &serenity::prelude::Context,
        options: &[CommandDataOption],
    ) -> Result<CreateInteractionResponseMessage> {
        let choice: usize = options
            .iter()
            .find(|o| o.name == "challenge")
            .map_or(0, |o| match &o.value {
                CommandDataOptionValue::Number(s) => s.floor() as usize,
                CommandDataOptionValue::Integer(s) => usize::try_from(*s).unwrap_or(0),
                unknown_option => {
                    error!("Didn't get a number, instead got {:?}", unknown_option);
                    0
                }
            });
        if choice == 0 {
            return Result::Err(anyhow!(
                "Unable to handle the result sent for whatever reason. Bug Araam.".to_string(),
            ));
        }

        // You can update the list using https://wiki.leagueoflegends.com/en-us/Challenges#Faction_Challenges
        let map = HashMap::from([
            (
                303_501,
                vec![
                    riven::consts::Champion::CORKI,
                    riven::consts::Champion::FIZZ,
                    riven::consts::Champion::GNAR,
                    riven::consts::Champion::HEIMERDINGER,
                    riven::consts::Champion::KENNEN,
                    riven::consts::Champion::KLED,
                    riven::consts::Champion::LULU,
                    riven::consts::Champion::POPPY,
                    riven::consts::Champion::RUMBLE,
                    riven::consts::Champion::TEEMO,
                    riven::consts::Champion::TRISTANA,
                    riven::consts::Champion::VEIGAR,
                    riven::consts::Champion::VEX,
                    riven::consts::Champion::YUUMI,
                    riven::consts::Champion::ZIGGS,
                ],
            ),
            (
                303_502,
                vec![
                    riven::consts::Champion::FIZZ,
                    riven::consts::Champion::GANGPLANK,
                    riven::consts::Champion::GRAVES,
                    riven::consts::Champion::ILLAOI,
                    riven::consts::Champion::MISS_FORTUNE,
                    riven::consts::Champion::NAUTILUS,
                    riven::consts::Champion::NILAH,
                    riven::consts::Champion::PYKE,
                    riven::consts::Champion::TAHM_KENCH,
                    riven::consts::Champion::TWISTED_FATE,
                ],
            ),
            (
                303_503,
                vec![
                    riven::consts::Champion::FIORA,
                    riven::consts::Champion::GALIO,
                    riven::consts::Champion::GAREN,
                    riven::consts::Champion::JARVAN_IV,
                    riven::consts::Champion::KAYLE,
                    riven::consts::Champion::LUCIAN,
                    riven::consts::Champion::LUX,
                    riven::consts::Champion::MORGANA,
                    riven::consts::Champion::POPPY,
                    riven::consts::Champion::QUINN,
                    riven::consts::Champion::SHYVANA,
                    riven::consts::Champion::SONA,
                    riven::consts::Champion::SYLAS,
                    riven::consts::Champion::VAYNE,
                    riven::consts::Champion::XIN_ZHAO,
                ],
            ),
            (
                303_504,
                vec![
                    riven::consts::Champion::ANIVIA,
                    riven::consts::Champion::ASHE,
                    riven::consts::Champion::AURORA,
                    riven::consts::Champion::BRAUM,
                    riven::consts::Champion::GNAR,
                    riven::consts::Champion::GRAGAS,
                    riven::consts::Champion::LISSANDRA,
                    riven::consts::Champion::NUNU_WILLUMP,
                    riven::consts::Champion::OLAF,
                    riven::consts::Champion::ORNN,
                    riven::consts::Champion::SEJUANI,
                    riven::consts::Champion::TRUNDLE,
                    riven::consts::Champion::TRYNDAMERE,
                    riven::consts::Champion::UDYR,
                    riven::consts::Champion::VOLIBEAR,
                ],
            ),
            (
                303_505,
                vec![
                    riven::consts::Champion::AHRI,
                    riven::consts::Champion::AKALI,
                    riven::consts::Champion::IRELIA,
                    riven::consts::Champion::IVERN,
                    riven::consts::Champion::HWEI,
                    riven::consts::Champion::JHIN,
                    riven::consts::Champion::KARMA,
                    riven::consts::Champion::KAYN,
                    riven::consts::Champion::KENNEN,
                    riven::consts::Champion::LEE_SIN,
                    riven::consts::Champion::LILLIA,
                    riven::consts::Champion::MASTER_YI,
                    riven::consts::Champion::RAKAN,
                    riven::consts::Champion::SETT,
                    riven::consts::Champion::SHEN,
                    riven::consts::Champion::SYNDRA,
                    riven::consts::Champion::VARUS,
                    riven::consts::Champion::WUKONG,
                    riven::consts::Champion::XAYAH,
                    riven::consts::Champion::YASUO,
                    riven::consts::Champion::YONE,
                    riven::consts::Champion::ZED,
                ],
            ),
            (
                303_506,
                vec![
                    riven::consts::Champion::MALPHITE,
                    riven::consts::Champion::MILIO,
                    riven::consts::Champion::NEEKO,
                    riven::consts::Champion::NIDALEE,
                    riven::consts::Champion::QIYANA,
                    riven::consts::Champion::RENGAR,
                    riven::consts::Champion::SKARNER,
                    riven::consts::Champion::ZYRA,
                ],
            ),
            (
                303_507,
                vec![
                    riven::consts::Champion::AMBESSA,
                    riven::consts::Champion::BRIAR,
                    riven::consts::Champion::CASSIOPEIA,
                    riven::consts::Champion::DARIUS,
                    riven::consts::Champion::DRAVEN,
                    riven::consts::Champion::KATARINA,
                    riven::consts::Champion::KLED,
                    riven::consts::Champion::LE_BLANC,
                    riven::consts::Champion::MORDEKAISER,
                    riven::consts::Champion::RELL,
                    riven::consts::Champion::RIVEN,
                    riven::consts::Champion::SAMIRA,
                    riven::consts::Champion::SION,
                    riven::consts::Champion::SWAIN,
                    riven::consts::Champion::TALON,
                    riven::consts::Champion::VLADIMIR,
                ],
            ),
            (
                303_508,
                vec![
                    riven::consts::Champion::CAITLYN,
                    riven::consts::Champion::CAMILLE,
                    riven::consts::Champion::CORKI,
                    riven::consts::Champion::EZREAL,
                    riven::consts::Champion::HEIMERDINGER,
                    riven::consts::Champion::JAYCE,
                    riven::consts::Champion::ORIANNA,
                    riven::consts::Champion::SERAPHINE,
                    riven::consts::Champion::VI,
                ],
            ),
            (
                303_509,
                vec![
                    riven::consts::Champion::ELISE,
                    riven::consts::Champion::FIDDLESTICKS,
                    riven::consts::Champion::GWEN,
                    riven::consts::Champion::HECARIM,
                    riven::consts::Champion::KALISTA,
                    riven::consts::Champion::KARTHUS,
                    riven::consts::Champion::MAOKAI,
                    riven::consts::Champion::SENNA,
                    riven::consts::Champion::THRESH,
                    riven::consts::Champion::VEX,
                    riven::consts::Champion::VIEGO,
                    riven::consts::Champion::YORICK,
                ],
            ),
            (
                303_510,
                vec![
                    riven::consts::Champion::AKSHAN,
                    riven::consts::Champion::AMUMU,
                    riven::consts::Champion::AZIR,
                    riven::consts::Champion::K_SANTE,
                    riven::consts::Champion::NAAFIRI,
                    riven::consts::Champion::NASUS,
                    riven::consts::Champion::RAMMUS,
                    riven::consts::Champion::RENEKTON,
                    riven::consts::Champion::SIVIR,
                    riven::consts::Champion::SKARNER,
                    riven::consts::Champion::TALIYAH,
                    riven::consts::Champion::XERATH,
                ],
            ),
            (
                303_511,
                vec![
                    riven::consts::Champion::APHELIOS,
                    riven::consts::Champion::AURELION_SOL,
                    riven::consts::Champion::DIANA,
                    riven::consts::Champion::LEONA,
                    riven::consts::Champion::PANTHEON,
                    riven::consts::Champion::SORAKA,
                    riven::consts::Champion::TARIC,
                    riven::consts::Champion::ZOE,
                ],
            ),
            (
                303_512,
                vec![
                    riven::consts::Champion::BEL_VETH,
                    riven::consts::Champion::CHO_GATH,
                    riven::consts::Champion::KAI_SA,
                    riven::consts::Champion::KASSADIN,
                    riven::consts::Champion::KHA_ZIX,
                    riven::consts::Champion::KOG_MAW,
                    riven::consts::Champion::MALZAHAR,
                    riven::consts::Champion::REK_SAI,
                    riven::consts::Champion::VEL_KOZ,
                ],
            ),
            (
                303_513,
                vec![
                    riven::consts::Champion::BLITZCRANK,
                    riven::consts::Champion::DR_MUNDO,
                    riven::consts::Champion::EKKO,
                    riven::consts::Champion::JANNA,
                    riven::consts::Champion::JINX,
                    riven::consts::Champion::RENATA_GLASC,
                    riven::consts::Champion::SINGED,
                    riven::consts::Champion::TWITCH,
                    riven::consts::Champion::URGOT,
                    riven::consts::Champion::VIKTOR,
                    riven::consts::Champion::WARWICK,
                    riven::consts::Champion::ZAC,
                    riven::consts::Champion::ZERI,
                    riven::consts::Champion::ZIGGS,
                ],
            ),
        ]);

        if let Some(champs_list) = map.get(&choice) {
            return Result::Ok(CreateInteractionResponseMessage::new().content(format!(
                "Here are the champs required to complete the challenge:\n\n{}",
                champs_list
                    .iter()
                    .map(|c| format!("- {}", c.name().unwrap_or("Unknown")))
                    .collect::<Vec<_>>()
                    .join("\n"))));
        }
        Result::Err(anyhow!("No idea lol".to_string()))
    }
}

pub mod groups {
    use crate::Config;
    use anyhow::anyhow;
    use anyhow::Result;
    use rand::seq::SliceRandom;
    use rand::thread_rng;
    use regex::Regex;
    use serenity::all::CommandDataOption;
    use serenity::all::CommandDataOptionValue;
    use serenity::all::CreateCommand;
    use serenity::all::CreateCommandOption;
    use serenity::all::CreateInteractionResponseMessage;
    use serenity::builder;
    use serenity::model::application::CommandOptionType;
    use serenity::model::guild::Member;
    use std::sync::Arc;
    use std::{collections::HashSet, vec};
    use tracing::{debug, error};

    use super::f64_to_usize;

    pub fn register() -> builder::CreateCommand {
        CreateCommand::new("groups")
            .description(
                "Generates random groups, based on the people currently in the Games channel.",
            )
            .add_option(CreateCommandOption::new(CommandOptionType::String, "ignore", "The users to ignore")
                    .required(false)
        )
            .add_option(CreateCommandOption::new(CommandOptionType::Number, "number_of_groups", "The amount of groups to create. Can't use with option max users.")
                    .required(false))
            .add_option(CreateCommandOption::new(CommandOptionType::Number, "max_users", "The maximum amount of users in a group. Can't use with option number of groups.")
                    .required(false))
    }

    pub async fn run(
        ctx: &serenity::prelude::Context,
        options: &[CommandDataOption],
        config: Arc<Config>,
    ) -> Result<CreateInteractionResponseMessage> {
        let channel_fetch = ctx.http.get_channel(config.voice_channel_id).await;
        let channel;
        if let Result::Ok(c) = channel_fetch {
            channel = c;
        } else {
            debug!("Voice channel missing");
            return Err(anyhow!("Unable to find the voice channel.".to_string()));
        };
        let active_members: Vec<Member> = match channel {
            serenity::model::prelude::Channel::Guild(guild_channel) => {
                match guild_channel.members(ctx.cache.as_ref()) {
                    Result::Ok(m) => m,
                    Result::Err(e) => {
                        error!("Error encountered while fetching members: {:?}", e);
                        vec![]
                    }
                }
            }
            c => {
                debug!("Just not a guild channel - was {}", c);
                vec![]
            }
        };
        if active_members.is_empty() {
            debug!("Unable to determine who to remove from the groups");
        }

        let re = Regex::new(r"<@(\d+)>")?;
        let mut ids_to_ignore = HashSet::<u64>::new();
        for (_, [user_id]) in
            re.captures_iter(options.iter().find(|o| o.name == "ignore").map_or("", |o| {
                match &o.value {
                    CommandDataOptionValue::String(s) => s,
                    _ => "",
                }
            }))
            .map(|c| c.extract())
        {
            ids_to_ignore.insert(if let Result::Ok(id) = user_id.parse::<u64>() {
                id
            } else {
                error!("Was unable to parse the user id {user_id}");
                0
            });
        }

        let number_of_groups = options
            .iter()
            .find(|o| o.name == "number_of_groups")
            .and_then(|o| match &o.value {
                CommandDataOptionValue::Number(n) => Some(n),
                _ => None,
            });

        let max_users = options
            .iter()
            .find(|o| o.name == "max_users")
            .and_then(|o| match &o.value {
                CommandDataOptionValue::Number(n) => Some(n),
                _ => None,
            });

        let members_to_group = active_members
            .into_iter()
            .filter(|member| !ids_to_ignore.contains(&member.user.id.get()))
            .collect::<Vec<Member>>();

        let total_groups: usize = match (number_of_groups, max_users) {
            (None, None) => 2,
            (Some(count), None) => match f64_to_usize(*count) {
                Result::Ok(i) => i,
                Result::Err(e) => return Err(e),
            },
            (None, Some(count)) => {
                let mut max_users_per_group = match f64_to_usize(*count) {
                    Result::Ok(i) => i,
                    Result::Err(e) => return Err(e),
                };

                if max_users_per_group > members_to_group.len() {
                    max_users_per_group = members_to_group.len();
                }
                members_to_group.len().div_ceil(max_users_per_group)
            }
            (Some(_), Some(_)) => {
                return Err(anyhow!(
                    "Can't provide both max users and number of groups.".to_owned()
                ))
            }
        };

        generate(&members_to_group, total_groups, &mut thread_rng())
    }

    pub fn generate<R: rand::RngCore>(
        members_to_group: &[Member],
        number_of_groups: usize,
        rng: &mut R,
    ) -> Result<CreateInteractionResponseMessage> {
        let mut members_to_group = members_to_group.to_owned();
        members_to_group.shuffle(rng);

        let mut results = Vec::new();
        if members_to_group.len() < number_of_groups {
            return Err(anyhow!(
                "Too many groups requested, and not enough users.".to_owned()
            ));
        }

        let quotient = members_to_group.len() / number_of_groups;
        let remainder = members_to_group.len() % number_of_groups;
        let split: usize = (quotient + 1) * remainder;
        let members_iter = members_to_group[..split]
            .chunks(quotient + 1)
            .chain(members_to_group[split..].chunks(quotient))
            .enumerate();

        for (group_count, members) in members_iter {
            results.push(format!("Group {}:", group_count + 1));

            results.push(
                members
                    .iter()
                    .map(|m| format!("<@{}>", m.user.id.get()))
                    .collect::<Vec<_>>()
                    .join(" "),
            );
        }

        Ok(CreateInteractionResponseMessage::new().content(results.join("\n\n")))
    }
}

pub mod register {
    use crate::db;
    use crate::riot_utils::parse_summoner_input;
    use crate::riot_utils::GameId;
    use crate::Config;
    use anyhow::{anyhow, Result};
    use riven::RiotApi;
    use serenity::all::CommandDataOption;
    use serenity::all::CommandDataOptionValue;
    use serenity::all::CreateCommandOption;
    use serenity::all::CreateInteractionResponseMessage;
    use serenity::builder;
    use serenity::model::application::CommandOptionType;
    use std::sync::Arc;
    pub fn register() -> builder::CreateCommand {
        builder::CreateCommand::new("register")
            .description("Adds a new user to the database.")
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::User,
                    "user",
                    "The Discord user to add",
                )
                .required(true),
            )
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::String,
                    "summoner",
                    "The summoner name / riot id.",
                )
                .required(true),
            )
    }

    pub async fn run(
        ctx: &serenity::prelude::Context,
        options: &[CommandDataOption],
        config: Arc<Config>,
    ) -> Result<CreateInteractionResponseMessage> {
        let riot_api = RiotApi::new(&config.riot_api_token);
        let pool = &config.pool;

        let user_id = match options.iter().find(|o| o.name == "user") {
            Some(user_option) => match &user_option.value {
                CommandDataOptionValue::User(user) => *user,
                _ => return Err(anyhow!("Expected a user, found something else.".to_string())),
            },
            None => {
                return Err(anyhow!("Looks like no user id specified.".to_string()));
            }
        };

        let user: serenity::all::User = ctx.http.get_user(user_id).await?;

        let GameId { name, tag } = match options.iter().find(|o| o.name == "summoner") {
            Some(account_option) => match &account_option.value {
                CommandDataOptionValue::String(name) => parse_summoner_input(name)?,
                _ => return Err(anyhow!("No name specified".to_string())),
            },
            None => return Err(anyhow!("No name specified".to_string())),
        };

        let account = riot_api
            .account_v1()
            .get_by_riot_id(riven::consts::RegionalRoute::AMERICAS, &name, &tag)
            .await?;
        let summoner_info = match account {
            Some(account) => {
                riot_api
                    .summoner_v4()
                    .get_by_puuid(riven::consts::PlatformRoute::NA1, &account.puuid)
                    .await?
            }
            None => {
                return Err(anyhow!("Unable to find the matching account".to_string()));
            }
        };

        let summoner_name_lower = format!("{name}#{tag}");

        db::register_user_summoner(pool, &user, &summoner_name_lower, &summoner_info).await?;

        // Return a success message
        Ok(CreateInteractionResponseMessage::new().content(format!(
            "Successfully registered Discord user '{}' with Riot account '{}'.",
            user.name, summoner_name_lower
        )))
    }
}

pub mod clash {
    use crate::riot_utils::GameId;
    use anyhow::{anyhow, Result};
    use riven::RiotApi;
    use serenity::all::CreateCommandOption;
    use serenity::all::CreateInteractionResponseMessage;
    use serenity::builder;
    use serenity::model::application::CommandDataOption;
    use serenity::model::application::CommandOptionType;
    use std::sync::Arc;

    use crate::Config;

    #[allow(dead_code)]
    pub fn register() -> builder::CreateCommand {
        builder::CreateCommand::new("clash")
            .description("Looks up clash team info based on a player on the enemy team.")
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::String,
                    "summoner",
                    "The summoner name / riot id.",
                )
                .required(true),
            )
    }

    #[allow(dead_code)]
    pub async fn run(
        _ctx: &serenity::prelude::Context,
        options: &[CommandDataOption],
        config: Arc<Config>,
    ) -> Result<CreateInteractionResponseMessage> {
        let riot_api = RiotApi::new(&config.riot_api_token);

        let GameId { name, tag } = match options.iter().find(|o| o.name == "summoner") {
            Some(account_option) => match account_option.value.as_str() {
                Some(name) => {
                    let text = name.trim();
                    if text.is_empty() {
                        return Err(anyhow!("No name specified".to_string()));
                    }
                    if text.contains('#') {
                        let mut parts = text.split('#');
                        GameId {
                            name: parts
                                .next()
                                .ok_or(anyhow!("No display name specified"))?
                                .to_string(),
                            tag: parts
                                .next()
                                .ok_or(anyhow!("No Riot tag specified"))?
                                .to_string(),
                        }
                    } else {
                        return Err(anyhow!("No name specified".to_string()));
                    }
                }
                None => return Err(anyhow!("No name specified".to_string())),
            },
            None => return Err(anyhow!("No name specified".to_string())),
        };

        let account = riot_api
            .account_v1()
            .get_by_riot_id(riven::consts::RegionalRoute::AMERICAS, &name, &tag)
            .await?;
        let member_info = match account {
            Some(account) => {
                riot_api
                    .summoner_v4()
                    .get_by_puuid(riven::consts::PlatformRoute::NA1, &account.puuid)
                    .await?
            }
            None => {
                return Err(anyhow!("Unable to find the matching account".to_string()));
            }
        };

        let clash_players = riot_api
            .clash_v1()
            .get_players_by_puuid(riven::consts::PlatformRoute::NA1, &member_info.puuid)
            .await?;
        let clash_tournaments = riot_api
            .clash_v1()
            .get_tournaments(riven::consts::PlatformRoute::NA1)
            .await?;
        let _ = clash_tournaments.iter().filter(|t| {
            t.schedule
                .iter()
                .any(|phase| phase.start_time <= chrono::Local::now().timestamp_micros())
        });
        let _team_ids = clash_players
            .iter()
            .filter_map(|player| player.team_id.as_ref())
            .map(|team_id| async {
                let _team_fetch = riot_api
                    .clash_v1()
                    .get_team_by_id(riven::consts::PlatformRoute::NA1, team_id)
                    .await
                    .unwrap_or_default();
            });

        Ok(CreateInteractionResponseMessage::new().content(""))
    }
}

pub mod rank {
    use crate::db;
    use crate::riot_utils::{parse_summoner_input, riot_api, GameId};
    use crate::Config;
    use anyhow::{anyhow, Context, Result};
    use riven::consts::{PlatformRoute, QueueType, RegionalRoute};
    use riven::RiotApi;
    use serenity::all::{
        CommandDataOption, CommandDataOptionValue, CreateCommand, CreateCommandOption, CreateEmbed,
        CreateInteractionResponseMessage, User,
    };
    use serenity::builder;
    use serenity::model::application::CommandOptionType;
    use std::collections::HashSet;
    use std::sync::Arc;
    use tracing::error;

    pub fn register() -> builder::CreateCommand {
        CreateCommand::new("rank")
            .description("Fetches the Ranked Solo/Duo and Flex queue stats for a player.")
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::String,
                    "summoner",
                    "The summoner name or Riot ID (Name#Tag). Optional: Defaults to your registered account.",
                )
                .required(false), // Make it optional
            )
    }

    pub async fn run(
        _ctx: &serenity::prelude::Context,
        options: &[CommandDataOption],
        config: Arc<Config>,
        caller: &User, // Pass the user who invoked the command
    ) -> Result<CreateInteractionResponseMessage> {
        // Return CreateEmbed for richer output
        let riot_api = RiotApi::new(&config.riot_api_token);
        let pool = &config.pool;

        let target_account_input: Option<&str> =
            options.iter().find(|o| o.name == "summoner").and_then(|o| {
                if let CommandDataOptionValue::String(s) = &o.value {
                    Some(s.as_str())
                } else {
                    None
                }
            });

        let (puuid, display_name) = match target_account_input {
            // --- User provided a summoner name/Riot ID ---
            Some(input_str) => {
                let GameId { name, tag } = parse_summoner_input(input_str)?;
                let display_name = input_str.to_string(); // Use the raw input for display initially
                let account_result = riot_api!(
                    riot_api
                        .account_v1()
                        .get_by_riot_id(RegionalRoute::AMERICAS, &name, &tag)
                        .await
                );

                let account_puuid = match account_result {
                    Ok(Some(acc)) => Ok(acc.puuid),
                    Ok(None) => Err(anyhow!("Riot ID `{name}#{tag}` not found.")),
                    Err(e) => Err(anyhow!("Error fetching Riot ID: {}", e)),
                }?;
                (account_puuid, display_name)
            }
            // --- No summoner provided, look up registered user ---
            None => {
                let registered_summoners = db::fetch_summoners(pool)
                    .await
                    .context("Failed to fetch registered summoners from DB")?;

                if let Some(summoner_name_or_riot_id) = registered_summoners.get(&caller.id.get()) {
                    // Grab puuid from the database.
                    let summoner_names =
                        HashSet::from([registered_summoners[&caller.id.get()].clone()]);
                    let puuids = db::fetch_puuids(pool, &summoner_names)
                        .await
                        .context("Failed to fetch PUUID from DB")?;
                    // grab the only puuid from the hashset of puuids.
                    let account_puuid = puuids
                        .iter()
                        .next()
                        .ok_or(anyhow!("No PUUID found for the registered summoner."))?;
                    (account_puuid.to_string(), summoner_name_or_riot_id.clone())
                } else {
                    return Err(anyhow!(
                        "You are not registered. Use `/register` or provide a summoner name/Riot ID."
                    ));
                }
            }
        };

        let league_entries_result = riot_api!(
            riot_api
                .league_v4()
                .get_league_entries_by_puuid(PlatformRoute::NA1, &puuid)
                .await
        );

        let league_entries = match league_entries_result {
            Ok(entries) => entries,
            Err(e) => return Err(anyhow!("Error fetching rank information: {}", e)),
        };

        // --- Build the Embed ---
        let mut embed = CreateEmbed::new()
            .title(format!("Ranked Stats for {}", display_name))
            .color(0x0099FF);

        let mut found_rank = false;

        for entry in league_entries {
            let queue_name = match entry.queue_type {
                QueueType::RANKED_SOLO_5x5 => "Ranked Solo/Duo",
                QueueType::RANKED_FLEX_SR => "Ranked Flex 5v5",
                _ => continue,
            };
            found_rank = true;

            let tier = entry
                .tier
                .map_or("Unranked".to_string(), |t| format!("{:?}", t));
            let rank = entry.rank.map_or("".to_string(), |r| format!("{:?}", r));
            let lp = entry.league_points;
            let wins = entry.wins;
            let losses = entry.losses;
            let win_rate = if wins + losses > 0 {
                (wins as f64 / (wins + losses) as f64) * 100.0
            } else {
                0.0
            };

            let rank_str = if tier != "Unranked" {
                format!("{} {}", tier, rank)
            } else {
                tier // Just "Unranked"
            };

            embed = embed.field(
                queue_name,
                format!(
                    "**{}**\n{} LP\n{:.1}% WR ({}W / {}L)",
                    rank_str, lp, win_rate, wins, losses
                ),
                true, // Inline field
            );
        }

        if !found_rank {
            embed = embed.description("This player is unranked in Solo/Duo and Flex queues.");
        }

        Ok(CreateInteractionResponseMessage::new().embed(embed))
    }
}

#[cfg(test)]
mod tests {

    use rand::{rngs::SmallRng, SeedableRng};
    use serenity::model::prelude::Member;

    use crate::commands::groups::generate;

    macro_rules! assert_content {
        ($content:expr, $value:expr) => {
            assert_eq!(
                $content,
                serde_json::to_value($value.unwrap())
                    .unwrap()
                    .as_object()
                    .unwrap()
                    .get("content")
                    .unwrap()
                    .as_str()
                    .unwrap()
            );
        };
    }

    fn member(id: u64, nick_name: &str) -> Member {
        serde_json::from_str(&format!(
            r#"{{
                "user": {{
                    "id": {id},
                    "username": "{nick_name}",
                    "discriminator": "1234"
                }},
                "roles": [],
                "joined_at": "2022-09-27 18:00:00.000-04:00",
                "deaf": false,
                "mute": false,
                "flags": 0,
                "guild_id": 1234
            }}"#,
        ))
        .unwrap()
    }

    #[test]
    fn splits_two_users_evenly() {
        let mut rng = SmallRng::seed_from_u64(528);
        let value = generate(
            &vec![member(123, "Test User"), member(234, "Test User 2")],
            2,
            &mut rng,
        );
        assert_content!("Group 1:\n\n<@234>\n\nGroup 2:\n\n<@123>", value);
    }

    #[test]
    fn splits_three_users_into_two_groups() {
        let mut rng: SmallRng = SmallRng::seed_from_u64(528);
        let value = generate(
            &vec![
                member(123, "Test User"),
                member(234, "Test User 2"),
                member(345, "Test User 3"),
            ],
            2,
            &mut rng,
        );
        assert_content!("Group 1:\n\n<@234> <@345>\n\nGroup 2:\n\n<@123>", value);
    }

    #[test]
    fn warns_if_too_many_groups() {
        let mut rng = SmallRng::seed_from_u64(528);
        let value = generate(
            &vec![
                member(123, "Test User"),
                member(234, "Test User 2"),
                member(345, "Test User 3"),
            ],
            4,
            &mut rng,
        );
        assert_eq!(
            "Too many groups requested, and not enough users.",
            value.unwrap_err().to_string()
        );
    }

    #[test]
    fn splits_into_more_than_two_groups() {
        let mut rng = SmallRng::seed_from_u64(528);
        let value = generate(
            &vec![
                member(123, "Test User"),
                member(234, "Test User 2"),
                member(345, "Test User 3"),
            ],
            3,
            &mut rng,
        );
        assert_content!(
            "Group 1:\n\n<@234>\n\nGroup 2:\n\n<@345>\n\nGroup 3:\n\n<@123>",
            value
        );
    }

    #[test]
    fn splitting_20_players_into_4_groups() {
        let mut rng = SmallRng::seed_from_u64(528);
        let value = generate(
            &(1..21)
                .map(|i| member(i, &format!("Test User {}", i)))
                .collect::<Vec<_>>(),
            4,
            &mut rng,
        );
        assert_content!("Group 1:\n\n<@9> <@8> <@7> <@6> <@12>\n\nGroup 2:\n\n<@2> <@14> <@1> <@19> <@10>\n\nGroup 3:\n\n<@13> <@5> <@17> <@16> <@18>\n\nGroup 4:\n\n<@11> <@15> <@20> <@4> <@3>", value);
    }
}
