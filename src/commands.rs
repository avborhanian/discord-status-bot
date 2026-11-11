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
                CommandDataOptionValue::String(name) => GameId::try_from(name.as_str())?,
                _ => return Err(anyhow!("No name specified".to_string())),
            },
            None => return Err(anyhow!("No name specified".to_string())),
        };

        let account = riot_api
            .account_v1()
            .get_by_riot_id(riven::consts::RegionalRoute::AMERICAS, &name, &tag)
            .await?;
        let summoner_info = match account {
            Some(account) => riot_api
                .summoner_v4()
                .get_by_puuid(riven::consts::PlatformRoute::NA1, &account.puuid)
                .await?
                .ok_or(anyhow!("Unable to find the matching summoner".to_string()))?,
            None => {
                return Err(anyhow!("Unable to find the matching account".to_string()));
            }
        };

        let tft_puuid = if !config.tft_riot_api_token.is_empty() {
            let tft_riot_api = RiotApi::new(&config.tft_riot_api_token);
            let tft_account = tft_riot_api
                .account_v1()
                .get_by_riot_id(riven::consts::RegionalRoute::AMERICAS, &name, &tag)
                .await?;
            tft_account.map(|a| a.puuid)
        } else {
            None
        };

        let summoner_name_lower = format!("{name}#{tag}");

        // Check if this summoner is already registered to a different Discord user
        if let Some(existing_owner_id) = db::find_existing_summoner_owner(pool, &summoner_name_lower).await? {
            if existing_owner_id != user_id.get().to_string() {
                return Err(anyhow!(
                    "The Riot account '{}' is already registered to a different Discord user. Use `/unregister` on that account first.",
                    summoner_name_lower
                ));
            }
        }

        db::register_user_summoner(
            pool,
            &user,
            &summoner_name_lower,
            &summoner_info,
            tft_puuid.as_deref(),
        )
        .await?;

        // Return a success message
        Ok(CreateInteractionResponseMessage::new().content(format!(
            "Successfully registered Discord user '{}' with Riot account '{}'.",
            user.name, summoner_name_lower
        )))
    }
}


pub mod unregister {
    use crate::db;
    use crate::Config;
    use anyhow::{anyhow, Result};
    use serenity::all::{
        CommandDataOption, CommandDataOptionValue, CreateCommand, CreateCommandOption,
        CreateInteractionResponseMessage,
    };
    use serenity::model::application::CommandOptionType;
    use std::sync::Arc;

    pub fn register() -> CreateCommand {
        CreateCommand::new("unregister")
            .description("Removes a Discord user's registered Riot account from the bot.")
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::User,
                    "user",
                    "The Discord user to unregister (defaults to yourself).",
                )
                .required(false),
            )
    }

    pub async fn run(
        ctx: &serenity::prelude::Context,
        options: &[CommandDataOption],
        config: Arc<Config>,
        caller: &serenity::model::user::User,
    ) -> Result<CreateInteractionResponseMessage> {
        let pool = &config.pool;

        let target_id = match options.iter().find(|o| o.name == "user") {
            Some(opt) => match &opt.value {
                CommandDataOptionValue::User(id) => {
                    ctx.http.get_user(*id).await?.id
                }
                _ => return Err(anyhow!("Expected a user.")),
            },
            None => caller.id,
        };

        match db::unregister_user(pool, target_id.get()).await? {
            true => Ok(CreateInteractionResponseMessage::new().content(format!(
                "Successfully unregistered <@{}>.",
                target_id
            ))),
            false => Err(anyhow!(
                "No registered account found for <@{}>.",
                target_id
            )),
        }
    }
}

pub mod list_users {
    use crate::db;
    use crate::Config;
    use anyhow::Result;
    use serenity::all::{CreateCommand, CreateEmbed, CreateInteractionResponseMessage};
    use std::sync::Arc;

    pub fn register() -> CreateCommand {
        CreateCommand::new("list-users")
            .description("Lists all Discord users registered with the bot.")
    }

    pub async fn run(
        _ctx: &serenity::prelude::Context,
        config: Arc<Config>,
    ) -> Result<CreateInteractionResponseMessage> {
        let pool = &config.pool;
        let summoners = db::fetch_summoners(pool).await?;

        if summoners.is_empty() {
            return Ok(CreateInteractionResponseMessage::new()
                .content("No users are currently registered."));
        }

        let mut lines: Vec<String> = summoners
            .iter()
            .map(|(discord_id, summoner_name)| format!("<@{}> → {}", discord_id, summoner_name))
            .collect();
        lines.sort();

        let embed = CreateEmbed::new()
            .title("Registered Users")
            .description(lines.join("\n"))
            .color(0x0099FF);

        Ok(CreateInteractionResponseMessage::new().embed(embed))
    }
}

pub mod rank {
    use crate::db;
    use crate::riot_utils::{riot_api, GameId};
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

        let tft_api: Option<RiotApi> = if config.tft_riot_api_token.is_empty() {
            None
        } else {
            Some(RiotApi::new(&config.tft_riot_api_token))
        };

        let target_account_input: Option<&str> =
            options.iter().find(|o| o.name == "summoner").and_then(|o| {
                if let CommandDataOptionValue::String(s) = &o.value {
                    Some(s.as_str())
                } else {
                    None
                }
            });

        let (puuid, tft_puuid, display_name) = match target_account_input {
            // --- User provided a summoner name/Riot ID ---
            Some(input_str) => {
                let GameId { name, tag } = GameId::try_from(input_str)?;
                let display_name = input_str.to_string();
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

                // Look up TFT PUUID via TFT API if available
                let tft_puuid = if let Some(ref tft) = tft_api {
                    tft.account_v1()
                        .get_by_riot_id(RegionalRoute::AMERICAS, &name, &tag)
                        .await
                        .ok()
                        .flatten()
                        .map(|acc| acc.puuid)
                } else {
                    None
                };

                (account_puuid, tft_puuid, display_name)
            }
            // --- No summoner provided, look up registered user ---
            None => {
                let registered_summoners = db::fetch_summoners(pool)
                    .await
                    .context("Failed to fetch registered summoners from DB")?;

                if let Some(summoner_name) = registered_summoners.get(&caller.id.get()) {
                    let summoner_names = HashSet::from([summoner_name.clone()]);
                    let puuid_map = db::fetch_summoner_puuids(pool, &summoner_names)
                        .await
                        .context("Failed to fetch PUUIDs from DB")?;
                    let (lol_puuid, tft_puuid) = puuid_map
                        .get(summoner_name)
                        .ok_or(anyhow!("No PUUID found for the registered summoner."))?;
                    (lol_puuid.clone(), tft_puuid.clone(), summoner_name.clone())
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
                tier
            };

            embed = embed.field(
                queue_name,
                format!(
                    "**{}**\n{} LP\n{:.1}% WR ({}W / {}L)",
                    rank_str, lp, win_rate, wins, losses
                ),
                true,
            );
        }

        if !found_rank {
            embed = embed.description("This player is unranked in Solo/Duo and Flex queues.");
        }

        // --- TFT Rank section ---
        if let (Some(tft_puuid), Some(tft)) = (&tft_puuid, &tft_api) {
            match tft.tft_league_v1().get_league_entries_by_puuid(PlatformRoute::NA1, tft_puuid).await {
                Ok(entries) => {
                    for entry in entries {
                        match entry.queue_type {
                            QueueType::RANKED_TFT => {
                                let tier = entry.tier.map_or("Unranked".to_string(), |t| format!("{:?}", t));
                                let rank = entry.rank.map_or("".to_string(), |r| format!("{:?}", r));
                                let lp = entry.league_points.unwrap_or(0);
                                let wins = entry.wins;
                                let losses = entry.losses;
                                let total = wins + losses;
                                let top4_rate = if total > 0 {
                                    (wins as f64 / total as f64) * 100.0
                                } else {
                                    0.0
                                };
                                let rank_str = if tier != "Unranked" {
                                    format!("{} {}", tier, rank)
                                } else {
                                    tier
                                };
                                embed = embed.field(
                                    "TFT Ranked",
                                    format!(
                                        "**{}**\n{} LP\n{:.1}% Top4 ({} / {} games)",
                                        rank_str, lp, top4_rate, wins, total
                                    ),
                                    true,
                                );
                            }
                            QueueType::RANKED_TFT_TURBO => {
                                let rated_tier = entry.rated_tier.as_deref().unwrap_or("Unrated");
                                let rated_rating = entry.rated_rating.unwrap_or(0);
                                let total = entry.wins + entry.losses;
                                embed = embed.field(
                                    "TFT Hyper Roll",
                                    format!(
                                        "**{}** ({})\n{} / {} games",
                                        rated_tier, rated_rating, entry.wins, total
                                    ),
                                    true,
                                );
                            }
                            QueueType::RANKED_TFT_DOUBLE_UP => {
                                let tier = entry.tier.map_or("Unranked".to_string(), |t| format!("{:?}", t));
                                let rank = entry.rank.map_or("".to_string(), |r| format!("{:?}", r));
                                let lp = entry.league_points.unwrap_or(0);
                                let rank_str = if tier != "Unranked" { format!("{} {}", tier, rank) } else { tier };
                                embed = embed.field(
                                    "TFT Double Up",
                                    format!("**{}**\n{} LP\n{} / {} games", rank_str, lp, entry.wins, entry.wins + entry.losses),
                                    true,
                                );
                            }
                            _ => {}
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to fetch TFT rank for {}: {}", display_name, e);
                }
            }
        }

        Ok(CreateInteractionResponseMessage::new().embed(embed))
    }
}

pub mod highlight {
    use crate::db;
    use crate::Config;
    use anyhow::{anyhow, Context, Result};
    use riven::consts::RegionalRoute;
    use riven::RiotApi;
    use serenity::all::{
        CommandDataOptionValue, CreateCommandOption, CreateInteractionResponseFollowup,
        CreateInteractionResponseMessage, EditInteractionResponse,
    };
    use serenity::builder;
    use serenity::model::application::CommandOptionType;
    use std::sync::Arc;
    use std::time::Duration;
    use tracing::{error, info};

    pub fn register() -> builder::CreateCommand {
        builder::CreateCommand::new("highlight")
            .description("Generates a highlight replay for a match (Admin only)")
            .default_member_permissions(serenity::model::Permissions::ADMINISTRATOR)
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::String,
                    "game_id",
                    "The match ID (e.g. 5676140397 or NA1_5676140397)",
                )
                .required(true),
            )
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::String,
                    "start",
                    "Start timestamp (e.g. '12:30' or '750's). Auto-detected if omitted.",
                )
                .required(false),
            )
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::Integer,
                    "duration",
                    "Clip duration in seconds (default: 25s, max: 60s)",
                )
                .required(false),
            )
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::String,
                    "player",
                    "Player name to focus camera on (auto-detected if omitted)",
                )
                .required(false),
            )
    }

    /// Parses a user-supplied timestamp string into milliseconds.
    /// Supports "MM:SS" (e.g. "12:30"), "SS" (e.g. "750"), "750s", or float seconds.
    pub fn parse_timestamp_ms(input: &str) -> Option<i64> {
        let s = input.trim();
        if let Some((m, sec)) = s.split_once(':') {
            let mins: f64 = m.trim().parse().ok()?;
            let secs: f64 = sec.trim().parse().ok()?;
            return Some(((mins * 60.0 + secs) * 1000.0) as i64);
        }
        let cleaned = s.trim_end_matches(|c: char| c == 's' || c == 'S');
        if let Ok(secs) = cleaned.parse::<f64>() {
            return Some((secs * 1000.0) as i64);
        }
        None
    }

    /// Searches an LCU timeline JSON structure for a pentakill event streak.
    /// If `target_killer_id` is specified, checks kills for that participant.
    /// Returns `Some((start_ms, end_ms, killer_id))`.
    pub fn find_penta_in_lcu_timeline(
        timeline: &serde_json::Value,
        target_killer_id: Option<i64>,
    ) -> Option<(i64, i64, i64)> {
        let frames = timeline.get("frames").and_then(serde_json::Value::as_array)?;
        let mut kills: Vec<(i64, i64)> = Vec::new(); // (timestamp, killer_id)

        for frame in frames {
            if let Some(events) = frame.get("events").and_then(serde_json::Value::as_array) {
                for event in events {
                    let ev_type = event
                        .get("type")
                        .or_else(|| event.get("eventType"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default();
                    if ev_type == "CHAMPION_KILL" {
                        let ts = event.get("timestamp").and_then(serde_json::Value::as_i64);
                        let killer = event
                            .get("killerId")
                            .or_else(|| event.get("killer_id"))
                            .and_then(serde_json::Value::as_i64);
                        if let (Some(ts), Some(killer)) = (ts, killer) {
                            if killer > 0 && (target_killer_id.is_none() || target_killer_id == Some(killer)) {
                                kills.push((ts, killer));
                            }
                        }
                    }
                }
            }
        }

        kills.sort_by_key(|(ts, _)| *ts);

        // Group kills by killer_id
        let mut by_killer: std::collections::HashMap<i64, Vec<i64>> = std::collections::HashMap::new();
        for (ts, killer) in kills {
            by_killer.entry(killer).or_default().push(ts);
        }

        // Check each killer for a 5-kill streak within multikill timing
        // League multikill window: <=15s between first 4 kills, <=30s for the 5th kill
        for (killer, mut killer_kills) in by_killer {
            killer_kills.sort();
            if killer_kills.len() >= 5 {
                for window in killer_kills.windows(5) {
                    let first_four_fast = window[..4]
                        .windows(2)
                        .all(|pair| (pair[1] - pair[0]) <= 15_000 && pair[1] >= pair[0]);
                    let fifth_in_time = (window[4] - window[3]) <= 30_000 && window[4] >= window[3];

                    if first_four_fast && fifth_in_time {
                        let start = (window[0] - 10_000).max(0);
                        let end = window[4] + 5_000;
                        return Some((start, end, killer));
                    }
                }
            }
        }

        None
    }

    #[derive(Debug, Clone)]
    pub struct PlayerCandidate {
        pub name: String,
        pub participant_id: i32,
        pub penta_kills: i32,
        pub kills: i32,
        pub is_tracked: bool,
    }

    /// Resolves the primary player or pentakiller from Riot API, DB, or LCU Relay,
    /// prioritizing registered summoners from the database.
    pub async fn resolve_game_player(
        game_id: i64,
        match_id: &str,
        manual_player: Option<&str>,
        riot_api: &RiotApi,
        pool: &sqlx::SqlitePool,
        hub: &crate::relay_hub::RelayHub,
    ) -> Option<PlayerCandidate> {
        let registered: std::collections::HashSet<String> = db::fetch_discord_usernames(pool)
            .await
            .unwrap_or_default()
            .into_keys()
            .collect();

        let check_tracked = |name: &str, tag: Option<&str>| -> bool {
            if let Some(target) = manual_player {
                if name.eq_ignore_ascii_case(target) {
                    return true;
                }
                if let Some(tag_str) = tag {
                    if format!("{name}#{tag_str}").eq_ignore_ascii_case(target) {
                        return true;
                    }
                }
            }
            let lower_name = name.to_ascii_lowercase();
            if registered.contains(&lower_name) {
                return true;
            }
            if let Some(tag_str) = tag {
                let full = format!("{lower_name}#{}", tag_str.to_ascii_lowercase());
                if registered.contains(&full) {
                    return true;
                }
            }
            false
        };

        let mut candidates: Vec<PlayerCandidate> = Vec::new();

        // 1. Try Riot API
        if let Ok(Some(match_data)) = riot_api
            .match_v5()
            .get_match(RegionalRoute::AMERICAS, match_id)
            .await
        {
            for p in &match_data.info.participants {
                let name = if !p.riot_id_game_name.as_deref().unwrap_or_default().is_empty() {
                    p.riot_id_game_name.clone().unwrap()
                } else {
                    p.summoner_name.clone()
                };
                let tag = p.riot_id_tagline.as_deref();
                let is_tracked = check_tracked(&name, tag);
                candidates.push(PlayerCandidate {
                    name,
                    participant_id: p.participant_id,
                    penta_kills: p.penta_kills,
                    kills: p.kills,
                    is_tracked,
                });
            }
        }

        // 2. Try DB if no candidates yet
        if candidates.is_empty() {
            if let Ok(stored) = db::fetch_seen_events(pool, &[match_id.to_string()]).await {
                if let Some(info) = stored.get(match_id) {
                    if let Some(m) = &info.match_info {
                        for p in &m.info.participants {
                            let name = if !p.riot_id_game_name.as_deref().unwrap_or_default().is_empty() {
                                p.riot_id_game_name.clone().unwrap()
                            } else {
                                p.summoner_name.clone()
                            };
                            let tag = p.riot_id_tagline.as_deref();
                            let is_tracked = check_tracked(&name, tag);
                            candidates.push(PlayerCandidate {
                                name,
                                participant_id: p.participant_id,
                                penta_kills: p.penta_kills,
                                kills: p.kills,
                                is_tracked,
                            });
                        }
                    }
                }
            }
        }

        // 3. Try LCU Relay if no candidates yet
        if candidates.is_empty() {
            if let Ok(lcu_game) = hub.get_game(game_id).await {
                let id_map: std::collections::HashMap<i32, (String, String)> = lcu_game
                    .participant_identities
                    .into_iter()
                    .map(|i| (i.participant_id, (i.player.game_name, i.player.tag_line)))
                    .collect();

                for p in lcu_game.participants {
                    let (name, tag) = id_map
                        .get(&p.participant_id)
                        .cloned()
                        .unwrap_or_default();
                    let is_tracked = check_tracked(&name, Some(&tag));
                    candidates.push(PlayerCandidate {
                        name,
                        participant_id: p.participant_id,
                        penta_kills: p.stats.penta_kills,
                        kills: p.stats.kills,
                        is_tracked,
                    });
                }
            }
        }

        if candidates.is_empty() {
            return None;
        }

        // Prioritize:
        // 1. Tracked player with pentakill
        // 2. Any player with pentakill
        // 3. Tracked player with highest kills
        // 4. Any player with highest kills
        candidates.sort_by(|a, b| {
            let a_key = (a.is_tracked && a.penta_kills > 0, a.penta_kills > 0, a.is_tracked, a.kills);
            let b_key = (b.is_tracked && b.penta_kills > 0, b.penta_kills > 0, b.is_tracked, b.kills);
            b_key.cmp(&a_key)
        });

        candidates.into_iter().next()
    }

    /// Automatically discovers the pentakill window `(start_ms, end_ms, player_name)`
    /// using Riot API timeline or the connected LCU relay's timeline.
    pub async fn resolve_pentakill_highlight(
        game_id: i64,
        match_id: &str,
        manual_player: Option<&str>,
        riot_api: &RiotApi,
        pool: &sqlx::SqlitePool,
        hub: &crate::relay_hub::RelayHub,
    ) -> Result<(i64, i64, String)> {
        // Resolve the target player from the game first (prioritizing tracked summoners from the database)
        let target_candidate = resolve_game_player(game_id, match_id, manual_player, riot_api, pool, hub).await;
        let (target_name, target_pid) = match &target_candidate {
            Some(c) => (c.name.clone(), Some(c.participant_id as i64)),
            None => ("player".to_string(), None),
        };

        // 1. Try Riot API timeline (standard queues)
        if let Ok(Some(timeline)) = riot_api
            .match_v5()
            .get_timeline(RegionalRoute::AMERICAS, match_id)
            .await
        {
            let penta_event = timeline
                .info
                .frames
                .iter()
                .flat_map(|f| f.events.iter())
                .find(|e| {
                    e.multi_kill_length == Some(5)
                        && (target_pid.is_none() || e.killer_id == target_pid.map(|p| p as i32))
                })
                .or_else(|| {
                    // Fallback to any pentakill in timeline
                    timeline
                        .info
                        .frames
                        .iter()
                        .flat_map(|f| f.events.iter())
                        .find(|e| e.multi_kill_length == Some(5))
                });

            if let Some(penta) = penta_event {
                let all_kills: Vec<_> = timeline
                    .info
                    .frames
                    .iter()
                    .flat_map(|f| f.events.iter())
                    .filter(|e| {
                        penta.timestamp >= e.timestamp
                            && e.killer_id == penta.killer_id
                            && e.r#type == "CHAMPION_KILL"
                    })
                    .collect();
                let start = if all_kills.len() >= 5 {
                    (all_kills[all_kills.len() - 5].timestamp - 10_000).max(0)
                } else {
                    (penta.timestamp - 15_000).max(0)
                };
                let end = penta.timestamp + 5_000;

                return Ok((start, end, target_name));
            }
        }

        // 2. Try LCU game timeline via relay (supports Mayhem & custom games)
        if let Ok(timeline_val) = hub.game_timeline(game_id).await {
            if let Some((start, end, _)) = find_penta_in_lcu_timeline(&timeline_val, target_pid) {
                return Ok((start, end, target_name));
            }
            if target_pid.is_some() {
                if let Some((start, end, _)) = find_penta_in_lcu_timeline(&timeline_val, None) {
                    return Ok((start, end, target_name));
                }
            }
        }

        // 3. Fallback error
        match target_candidate {
            Some(c) => Err(anyhow!("Game found ({}), but no pentakill event was detected in the timeline", c.name)),
            None => Err(anyhow!("Match details not found in Riot API, database, or client")),
        }
    }

    pub async fn run(
        ctx: &serenity::prelude::Context,
        command: &serenity::model::application::CommandInteraction,
        config: Arc<Config>,
    ) -> Result<CreateInteractionResponseMessage> {
        let user = &command.user;
        let options = &command.data.options;
        let user_id_str = user.id.get().to_string();
        if !db::is_admin(&config.pool, &user_id_str).await? {
            return Ok(CreateInteractionResponseMessage::new()
                .content("⛔ Only administrators can trigger highlight replay generation.")
                .ephemeral(true));
        }

        let game_id_str = options
            .iter()
            .find(|o| o.name == "game_id")
            .and_then(|o| match &o.value {
                CommandDataOptionValue::String(s) => Some(s.clone()),
                _ => None,
            })
            .ok_or_else(|| anyhow!("Missing game_id argument"))?;

        let numeric_str = game_id_str
            .trim()
            .strip_prefix("NA1_")
            .unwrap_or(game_id_str.trim());
        let game_id: i64 = numeric_str
            .parse()
            .context("Invalid game_id: must be a number or NA1_<number>")?;

        let manual_start_str = options
            .iter()
            .find(|o| o.name == "start")
            .and_then(|o| match &o.value {
                CommandDataOptionValue::String(s) => Some(s.trim().to_string()),
                _ => None,
            });

        let manual_duration = options
            .iter()
            .find(|o| o.name == "duration")
            .and_then(|o| match &o.value {
                CommandDataOptionValue::Integer(i) => Some(*i),
                _ => None,
            });

        let manual_player = options
            .iter()
            .find(|o| o.name == "player")
            .and_then(|o| match &o.value {
                CommandDataOptionValue::String(s) => Some(s.trim().to_string()),
                _ => None,
            });

        let relay_hub = match &config.relay_hub {
            Some(hub) if hub.has_live_relay().await => hub.clone(),
            _ => {
                return Ok(CreateInteractionResponseMessage::new()
                    .content("❌ No connected computer with a running League client is available right now.")
                    .ephemeral(true));
            }
        };

        let match_id = format!("NA1_{game_id}");
        let riot_api = RiotApi::new(&config.riot_api_token);

        let (start_ms, end_ms, player_name, is_manual) = if let Some(ref s_str) = manual_start_str {
            let start = parse_timestamp_ms(s_str).ok_or_else(|| {
                anyhow!("Invalid start format '{s_str}'. Use MM:SS (e.g. 12:30) or seconds (e.g. 750).")
            })?;
            let duration_sec = manual_duration.unwrap_or(25).clamp(5, 60);
            let end = start + duration_sec * 1000;
            let player = match manual_player {
                Some(p) => p,
                None => resolve_game_player(game_id, &match_id, None, &riot_api, &config.pool, &relay_hub)
                    .await
                    .map(|c| c.name)
                    .unwrap_or_else(|| "player".to_string()),
            };
            (start, end, player, true)
        } else {
            match resolve_pentakill_highlight(
                game_id,
                &match_id,
                manual_player.as_deref(),
                &riot_api,
                &config.pool,
                &relay_hub,
            )
            .await
            {
                Ok((start, end, player)) => (start, end, player, false),
                Err(e) => {
                    return Ok(CreateInteractionResponseMessage::new()
                        .content(format!(
                            "⚠️ Could not automatically detect a Pentakill timeline for match `{match_id}`: {e}\n\n\
                             You can trigger highlight recording by providing the start timestamp manually:\n\
                             `/highlight game_id:{game_id} start:MM:SS` (e.g. `/highlight game_id:{game_id} start:12:30`)"
                        ))
                        .ephemeral(true));
                }
            }
        };

        let start_sec = start_ms / 1000;
        let time_display = format!("{:02}:{:02}", start_sec / 60, start_sec % 60);
        let mode_desc = if is_manual {
            format!("highlight clip at **{time_display}**")
        } else {
            "Pentakill highlight".to_string()
        };

        // Dispatch in background to a random connected computer
        let http = ctx.http.clone();
        let match_id_clone = match_id.clone();
        let player_name_clone = player_name.clone();
        let mode_desc_clone = mode_desc.clone();
        let command_clone = command.clone();
        let updates_channel = config.updates_channel_id;

        tokio::spawn(async move {
            info!("Requesting highlight for {match_id_clone} ({player_name_clone}, {start_ms}ms-{end_ms}ms) from random relay...");
            match relay_hub
                .request_highlight_from_random_relay(
                    game_id,
                    start_ms,
                    end_ms,
                    player_name_clone.clone(),
                    Duration::from_secs(180),
                )
                .await
            {
                Ok((relay_name, reply)) => {
                    let (status_text, followup_msg) = if reply.success {
                        let path = reply.file_path.as_deref().unwrap_or("Highlights folder");
                        (
                            format!(
                                "🎬 Dispatched {mode_desc_clone} for match `{match_id_clone}` (**{player_name_clone}**) — **Complete!** ✅"
                            ),
                            format!(
                                "🎬 **Highlight Replay Generated!**\nMatch: `{match_id_clone}`\nPlayer: **{player_name_clone}**\nGenerated on client: **{relay_name}**\nSaved to: `{path}`"
                            ),
                        )
                    } else {
                        let err = reply.error.as_deref().unwrap_or("unknown error");
                        (
                            format!(
                                "🎬 Dispatched {mode_desc_clone} for match `{match_id_clone}` (**{player_name_clone}**) — **Failed** ❌"
                            ),
                            format!(
                                "⚠️ Highlight generation failed on **{relay_name}** for `{match_id_clone}`: {err}"
                            ),
                        )
                    };
                    let _ = command_clone
                        .edit_response(&http, EditInteractionResponse::new().content(&status_text))
                        .await;
                    let followup = CreateInteractionResponseFollowup::new()
                        .content(&followup_msg)
                        .ephemeral(true);
                    let _ = command_clone.create_followup(&http, followup).await;

                    // If replay was recorded successfully, upload the video clip to the highlight reel!
                    if reply.success {
                        let attachment = reply.video_bytes.map(|b| {
                            let filename = format!("{match_id_clone}_{player_name_clone}.webm");
                            (b, filename)
                        });
                        let reel_msg = if attachment.is_some() {
                            format!(
                                "🎬 **Highlight Replay Generated!**\nMatch: `{match_id_clone}`\nPlayer: **{player_name_clone}**"
                            )
                        } else {
                            let path = reply.file_path.as_deref().unwrap_or("Highlights folder");
                            format!(
                                "🎬 **Highlight Replay Generated!**\nMatch: `{match_id_clone}`\nPlayer: **{player_name_clone}**\nSaved on client: `{path}`"
                            )
                        };
                        let _ = crate::upload_to_highlight_reel(
                            &reel_msg,
                            attachment,
                            &http,
                            updates_channel,
                        )
                        .await;
                    }
                }
                Err(e) => {
                    error!("Highlight generation failed for {match_id_clone}: {e}");
                    let status_text = format!(
                        "🎬 Dispatched {mode_desc_clone} for match `{match_id_clone}` (**{player_name_clone}**) — **Failed** ❌"
                    );
                    let _ = command_clone
                        .edit_response(&http, EditInteractionResponse::new().content(&status_text))
                        .await;
                    let msg = format!("❌ Highlight generation failed for `{match_id_clone}`: {e}");
                    let followup = CreateInteractionResponseFollowup::new()
                        .content(&msg)
                        .ephemeral(true);
                    let _ = command_clone.create_followup(&http, followup).await;
                }
            }
        });

        Ok(CreateInteractionResponseMessage::new()
            .content(format!(
                "🎬 Dispatched {mode_desc} for match `{match_id}` (**{player_name}**) to a random connected computer!\nRecording in progress...",
            ))
            .ephemeral(true))
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

    #[test]
    fn test_parse_timestamp_ms() {
        use super::highlight::parse_timestamp_ms;
        assert_eq!(parse_timestamp_ms("12:30"), Some(750_000));
        assert_eq!(parse_timestamp_ms("0:00"), Some(0));
        assert_eq!(parse_timestamp_ms("750"), Some(750_000));
        assert_eq!(parse_timestamp_ms("750s"), Some(750_000));
        assert_eq!(parse_timestamp_ms("750.5"), Some(750_500));
        assert_eq!(parse_timestamp_ms("invalid"), None);
    }

    #[test]
    fn test_find_penta_in_lcu_timeline() {
        use super::highlight::find_penta_in_lcu_timeline;
        let timeline = serde_json::json!({
            "frames": [
                {
                    "events": [
                        { "type": "CHAMPION_KILL", "timestamp": 100000, "killerId": 1 },
                        { "type": "CHAMPION_KILL", "timestamp": 105000, "killerId": 1 },
                        { "type": "CHAMPION_KILL", "timestamp": 110000, "killerId": 1 },
                        { "type": "CHAMPION_KILL", "timestamp": 115000, "killerId": 1 },
                        { "type": "CHAMPION_KILL", "timestamp": 120000, "killerId": 1 }
                    ]
                }
            ]
        });
        let result = find_penta_in_lcu_timeline(&timeline, None);
        assert_eq!(result, Some((90000, 125000, 1)));

        let result_specific = find_penta_in_lcu_timeline(&timeline, Some(1));
        assert_eq!(result_specific, Some((90000, 125000, 1)));

        let result_other = find_penta_in_lcu_timeline(&timeline, Some(2));
        assert_eq!(result_other, None);
    }
}
