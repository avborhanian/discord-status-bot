use anyhow::{anyhow, Result};

fn number_to_usize(count: &serde_json::Number) -> Result<usize> {
    Result::Ok(match (count.as_i64(), count.as_f64(), count.as_u64()) {
        (Some(i), None, None) => {
            if i <= 0 {
                return Result::Err(anyhow!(
                    "You should provide a positive number. Zero or less is not allowed."
                ));
            } else {
                i.try_into().unwrap()
            }
        }
        (None, Some(f), None) => {
            if f.round() <= 0.0 {
                return Result::Err(anyhow!(
                    "You should provide a positive number. Zero or less is not allowed."
                ));
            } else {
                (f.round() as u64).try_into().unwrap()
            }
        }
        (None, None, Some(u)) => u.try_into().unwrap(),
        _ => match count.as_str().parse() {
            Result::Ok(id) => id,
            Result::Err(_) => {
                return Result::Err(anyhow!("Unhandled state, please try again later."))
            }
        },
    })
}

enum GameId {
    SummonerName(String),
    RiotId(String, String),
}

pub mod globetrotters {
    use std::collections::HashMap;

    use serenity::builder;
    use serenity::model::prelude::application_command::CommandDataOption;
    use serenity::model::prelude::command::CommandOptionType;
    use serenity::prelude::Context;
    use tracing::error;

    use super::number_to_usize;

    pub fn register(
        command: &mut builder::CreateApplicationCommand,
    ) -> &mut builder::CreateApplicationCommand {
        command
            .name("globetrotters")
            .description("Returns a list of champions required to complete a challenge.")
            .create_option(|option| {
                option
                    .name("challenge")
                    .description("Which challenge/region you want to complete")
                    .kind(CommandOptionType::Integer)
                    .add_int_choice("5 Under 5' - Bandle City", 303501)
                    .add_int_choice("All Hands on Deck - Bilgewater", 303502)
                    .add_int_choice("FOR DEMACIA - Demacia", 303503)
                    .add_int_choice("Ice, Ice, Baby - the Freljord", 303504)
                    .add_int_choice("Everybody was Wuju Fighting - Ionia", 303505)
                    .add_int_choice("Elemental, My Dear Watson - Ixtal", 303506)
                    .add_int_choice("Strength Above All - Noxus", 303507)
                    .add_int_choice("Calculated - Piltover", 303508)
                    .add_int_choice("Spooky Scary Skeletons - the Shadow Isles", 303509)
                    .add_int_choice("The Sun Disc Never Sets - Shurima", 303510)
                    .add_int_choice("Peak Performance - Targon", 303511)
                    .add_int_choice("(Inhuman Screeching Sounds) - the Void", 303512)
                    .add_int_choice("Chemtech Comrades - Zaun", 303513)
                    .required(true)
            })
    }

    pub async fn run(_ctx: &Context, options: &[CommandDataOption]) -> String {
        let choice: usize = options
            .iter()
            .find(|o| o.name == "challenge")
            .map_or(0, |o| match &o.value {
                Some(serde_json::Value::Number(s)) => number_to_usize(s).unwrap_or(0),
                Some(s) => {
                    error!("Didn't get a number, instead got {}", s);
                    0
                }
                None => 0,
            });
        if choice == 0 {
            return "Unable to handle the result sent for whatever reason. Bug Araam.".to_string();
        }

        let map = HashMap::from([
            (
                303501,
                vec![
                    riven::consts::Champion::RUMBLE,
                    riven::consts::Champion::VEX,
                    riven::consts::Champion::FIZZ,
                    riven::consts::Champion::CORKI,
                    riven::consts::Champion::HEIMERDINGER,
                    riven::consts::Champion::VEIGAR,
                    riven::consts::Champion::POPPY,
                    riven::consts::Champion::KLED,
                    riven::consts::Champion::TEEMO,
                    riven::consts::Champion::TRISTANA,
                    riven::consts::Champion::ZIGGS,
                    riven::consts::Champion::LULU,
                    riven::consts::Champion::KENNEN,
                    riven::consts::Champion::GNAR,
                    riven::consts::Champion::YUUMI,
                ],
            ),
            (
                303502,
                vec![
                    riven::consts::Champion::TWISTED_FATE,
                    riven::consts::Champion::ILLAOI,
                    riven::consts::Champion::MISS_FORTUNE,
                    riven::consts::Champion::GRAVES,
                    riven::consts::Champion::GANGPLANK,
                    riven::consts::Champion::FIZZ,
                    riven::consts::Champion::PYKE,
                    riven::consts::Champion::NAUTILUS,
                    riven::consts::Champion::NILAH,
                    riven::consts::Champion::TAHM_KENCH,
                ],
            ),
            (
                303503,
                vec![
                    riven::consts::Champion::LUX,
                    riven::consts::Champion::GALIO,
                    riven::consts::Champion::VAYNE,
                    riven::consts::Champion::SHYVANA,
                    riven::consts::Champion::KAYLE,
                    riven::consts::Champion::LUCIAN,
                    riven::consts::Champion::POPPY,
                    riven::consts::Champion::FIORA,
                    riven::consts::Champion::GAREN,
                    riven::consts::Champion::MORGANA,
                    riven::consts::Champion::JARVAN_IV,
                ],
            ),
            (
                303504,
                [
                    riven::consts::Champion::OLAF,
                    riven::consts::Champion::ANIVIA,
                    riven::consts::Champion::ORNN,
                    riven::consts::Champion::BRAUM,
                    riven::consts::Champion::VOLIBEAR,
                    riven::consts::Champion::UDYR,
                    riven::consts::Champion::GRAGAS,
                    riven::consts::Champion::TRUNDLE,
                    riven::consts::Champion::SEJUANI,
                    riven::consts::Champion::NUNU_WILLUMP,
                    riven::consts::Champion::ASHE,
                    riven::consts::Champion::GNAR,
                    riven::consts::Champion::TRYNDAMERE,
                    riven::consts::Champion::LISSANDRA,
                ]
                .to_vec(),
            ),
            (
                303505,
                vec![
                    riven::consts::Champion::LEE_SIN,
                    riven::consts::Champion::SHEN,
                    riven::consts::Champion::SYNDRA,
                    riven::consts::Champion::IRELIA,
                    riven::consts::Champion::AHRI,
                    riven::consts::Champion::YONE,
                    riven::consts::Champion::JHIN,
                    riven::consts::Champion::SETT,
                    riven::consts::Champion::MASTER_YI,
                    riven::consts::Champion::IVERN,
                    riven::consts::Champion::KARMA,
                    riven::consts::Champion::LILLIA,
                    riven::consts::Champion::KAYN,
                    riven::consts::Champion::VARUS,
                    riven::consts::Champion::ZED,
                    riven::consts::Champion::RAKAN,
                    riven::consts::Champion::XAYAH,
                    riven::consts::Champion::AKALI,
                    riven::consts::Champion::KENNEN,
                    riven::consts::Champion::YASUO,
                    riven::consts::Champion::WUKONG,
                ],
            ),
            (
                303506,
                vec![
                    riven::consts::Champion::NEEKO,
                    riven::consts::Champion::QIYANA,
                    riven::consts::Champion::MALPHITE,
                    riven::consts::Champion::MILIO,
                    riven::consts::Champion::RENGAR,
                    riven::consts::Champion::NIDALEE,
                    riven::consts::Champion::ZYRA,
                ],
            ),
            (
                303507,
                vec![
                    riven::consts::Champion::CASSIOPEIA,
                    riven::consts::Champion::LE_BLANC,
                    riven::consts::Champion::SAMIRA,
                    riven::consts::Champion::VLADIMIR,
                    riven::consts::Champion::BRIAR,
                    riven::consts::Champion::SION,
                    riven::consts::Champion::RELL,
                    riven::consts::Champion::KLED,
                    riven::consts::Champion::SWAIN,
                    riven::consts::Champion::DRAVEN,
                    riven::consts::Champion::KATARINA,
                    riven::consts::Champion::DARIUS,
                    riven::consts::Champion::TALON,
                    riven::consts::Champion::RIVEN,
                ],
            ),
            (
                303508,
                vec![
                    riven::consts::Champion::EZREAL,
                    riven::consts::Champion::SERAPHINE,
                    riven::consts::Champion::CAITLYN,
                    riven::consts::Champion::CAMILLE,
                    riven::consts::Champion::CORKI,
                    riven::consts::Champion::HEIMERDINGER,
                    riven::consts::Champion::ORIANNA,
                    riven::consts::Champion::JAYCE,
                    riven::consts::Champion::VI,
                ],
            ),
            (
                303509,
                vec![
                    riven::consts::Champion::YORICK,
                    riven::consts::Champion::GWEN,
                    riven::consts::Champion::HECARIM,
                    riven::consts::Champion::FIDDLESTICKS,
                    riven::consts::Champion::MAOKAI,
                    riven::consts::Champion::VIEGO,
                    riven::consts::Champion::SENNA,
                    riven::consts::Champion::EVELYNN,
                    riven::consts::Champion::THRESH,
                    riven::consts::Champion::ELISE,
                    riven::consts::Champion::KALISTA,
                    riven::consts::Champion::KARTHUS,
                ],
            ),
            (
                303510,
                vec![
                    riven::consts::Champion::AMUMU,
                    riven::consts::Champion::K_SANTE,
                    riven::consts::Champion::RAMMUS,
                    riven::consts::Champion::TALIYAH,
                    riven::consts::Champion::XERATH,
                    riven::consts::Champion::AKSHAN,
                    riven::consts::Champion::SKARNER,
                    riven::consts::Champion::NASUS,
                    riven::consts::Champion::AZIR,
                    riven::consts::Champion::SIVIR,
                    riven::consts::Champion::NAAFIRI,
                    riven::consts::Champion::ZILEAN,
                    riven::consts::Champion::RENEKTON,
                ],
            ),
            (
                303511,
                vec![
                    riven::consts::Champion::SORAKA,
                    riven::consts::Champion::PANTHEON,
                    riven::consts::Champion::DIANA,
                    riven::consts::Champion::AURELION_SOL,
                    riven::consts::Champion::LEONA,
                    riven::consts::Champion::APHELIOS,
                    riven::consts::Champion::TARIC,
                    riven::consts::Champion::ZOE,
                ],
            ),
            (
                303512,
                vec![
                    riven::consts::Champion::KOG_MAW,
                    riven::consts::Champion::KAI_SA,
                    riven::consts::Champion::VEL_KOZ,
                    riven::consts::Champion::REK_SAI,
                    riven::consts::Champion::KASSADIN,
                    riven::consts::Champion::BEL_VETH,
                    riven::consts::Champion::KHA_ZIX,
                    riven::consts::Champion::MALZAHAR,
                    riven::consts::Champion::CHO_GATH,
                ],
            ),
            (
                303513,
                vec![
                    riven::consts::Champion::DR_MUNDO,
                    riven::consts::Champion::URGOT,
                    riven::consts::Champion::JANNA,
                    riven::consts::Champion::VIKTOR,
                    riven::consts::Champion::ZIGGS,
                    riven::consts::Champion::WARWICK,
                    riven::consts::Champion::EKKO,
                    riven::consts::Champion::BLITZCRANK,
                    riven::consts::Champion::RENATA_GLASC,
                    riven::consts::Champion::ZAC,
                    riven::consts::Champion::SINGED,
                    riven::consts::Champion::TWITCH,
                    riven::consts::Champion::ZERI,
                    riven::consts::Champion::JINX,
                ],
            ),
        ]);

        if map.contains_key(&choice) {
            let champs_list = map.get(&choice).unwrap();

            return format!(
                "Here are the champs required to complete the challenge:\n\n{}",
                champs_list
                    .iter()
                    .map(|c| format!("- {}", c.name().unwrap()))
                    .collect::<Vec<_>>()
                    .join("\n")
            );
        }
        "No idea lol".to_string()
    }
}

pub mod groups {
    use anyhow::Result;
    use rand::seq::SliceRandom;
    use rand::thread_rng;
    use regex::Regex;
    use serenity::builder;
    use serenity::model::guild::Member;
    use serenity::model::prelude::application_command::CommandDataOption;
    use serenity::model::prelude::command::CommandOptionType;
    use serenity::prelude::Context;
    use std::{collections::HashSet, env, vec};
    use tracing::{debug, error};

    use crate::commands::number_to_usize;

    pub fn register(
        command: &mut builder::CreateApplicationCommand,
    ) -> &mut builder::CreateApplicationCommand {
        command
            .name("groups")
            .description(
                "Generates random groups, based on the people currently in the Games channel.",
            )
            .create_option(|option| {
                option
                    .name("ignore")
                    .description("The users to ignore")
                    .kind(CommandOptionType::String)
                    .required(false)
            })
            .create_option(|option| {
                option
                    .name("number_of_groups")
                    .description("The amount of groups to create. Can't use with option max users.")
                    .kind(CommandOptionType::Number)
                    .required(false)
            })
            .create_option(|option| {
                option
                    .name("max_users")
                    .description("The maximum amount of users in a group. Can't use with option number of groups.")
                    .kind(CommandOptionType::Number)
                    .required(false)
            })
    }

    pub async fn run(ctx: &Context, options: &[CommandDataOption]) -> String {
        let channel_id = match env::var("VOICE_CHANNEL_ID") {
            Result::Ok(s) => match s.parse() {
                Result::Ok(id) => id,
                Err(_) => {
                    return "The provided id was not a valid u64 number.".to_string();
                }
            },
            Err(_) => {
                return "Unable to find the voice channel.".to_string();
            }
        };
        let channel_fetch = ctx.http.get_channel(channel_id).await;
        let channel;
        if let Result::Ok(c) = channel_fetch {
            channel = c;
        } else {
            debug!("Voice channel missing");
            return "Unable to find the voice channel.".to_string();
        };
        let active_members: Vec<Member> = match channel {
            serenity::model::prelude::Channel::Guild(guild_channel) => {
                match guild_channel.members(ctx.cache.as_ref()).await {
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

        let re = Regex::new(r"<@(\d+)>").unwrap();
        let mut ids_to_ignore = HashSet::<u64>::new();
        for (_, [user_id]) in
            re.captures_iter(options.iter().find(|o| o.name == "ignore").map_or("", |o| {
                match &o.value {
                    Some(serde_json::Value::String(s)) => s,
                    Some(_) => "",
                    None => todo!(),
                }
            }))
            .map(|c| c.extract())
        {
            ids_to_ignore.insert(match user_id.parse::<u64>() {
                Result::Ok(id) => id,
                Err(_) => {
                    error!("Was unable to parse the user id {}", user_id);
                    0
                }
            });
        }

        let number_of_groups = options
            .iter()
            .find(|o| o.name == "number_of_groups")
            .and_then(|o| match &o.value {
                Some(serde_json::Value::Number(n)) => Some(n),
                Some(_) => None,
                None => None,
            });

        let max_users = options
            .iter()
            .find(|o| o.name == "max_users")
            .and_then(|o| match &o.value {
                Some(serde_json::Value::Number(n)) => Some(n),
                Some(_) => None,
                None => None,
            });

        let members_to_group = active_members
            .into_iter()
            .filter(|member| !ids_to_ignore.contains(member.user.id.as_u64()))
            .collect::<Vec<Member>>();

        let total_groups: usize = match (number_of_groups, max_users) {
            (None, None) => 2,
            (Some(count), None) => match number_to_usize(count) {
                Result::Ok(i) => i,
                Result::Err(e) => return e.to_string(),
            },
            (None, Some(count)) => {
                let mut max_users_per_group = match number_to_usize(count) {
                    Result::Ok(i) => i,
                    Result::Err(e) => return e.to_string(),
                };

                if max_users_per_group > members_to_group.len() {
                    max_users_per_group = members_to_group.len();
                }
                members_to_group.len().div_ceil(max_users_per_group)
            }
            (Some(_), Some(_)) => {
                return "Can't provide both max users and number of groups.".to_owned()
            }
        };

        generate_groups(members_to_group, total_groups, &mut thread_rng())
    }

    pub fn generate_groups<R: rand::RngCore>(
        members_to_group: Vec<Member>,
        number_of_groups: usize,
        rng: &mut R,
    ) -> String {
        let mut members_to_group = members_to_group.clone();
        members_to_group.shuffle(rng);

        let mut results = Vec::new();
        if members_to_group.len() < number_of_groups {
            return "Too many groups requested, and not enough users.".to_owned();
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
                    .map(|m| format!("<@{}>", m.user.id.as_u64()))
                    .collect::<Vec<_>>()
                    .join(" "),
            );
        }

        results.join("\n\n")
    }
}

pub mod register {
    use riven::RiotApi;
    use serenity::builder;
    use serenity::model::application::interaction::application_command::CommandDataOptionValue;
    use serenity::model::prelude::application_command::CommandDataOption;
    use serenity::model::prelude::command::CommandOptionType;
    use serenity::prelude::Context;
    use std::env;

    use super::GameId;

    pub fn register(
        command: &mut builder::CreateApplicationCommand,
    ) -> &mut builder::CreateApplicationCommand {
        command
            .name("register")
            .description("Adds a new user to the database.")
            .create_option(|option| {
                option
                    .name("user")
                    .description("The Discord user to add")
                    .kind(CommandOptionType::User)
                    .required(true)
            })
            .create_option(|option| {
                option
                    .name("summoner")
                    .description("The summoner name / riot id.")
                    .kind(CommandOptionType::String)
                    .required(true)
            })
    }

    pub async fn run(_ctx: &Context, options: &[CommandDataOption]) -> String {
        let riot_api_key: &str = &env::var("RIOT_API_TOKEN").unwrap();
        let riot_api = RiotApi::new(riot_api_key);

        let user = match options.iter().find(|o| o.name == "user") {
            Some(user_option) => match &user_option.resolved {
                Some(value) => match value {
                    CommandDataOptionValue::User(user, _) => user,
                    _ => return "Expected a user, found something else.".to_string(),
                },
                None => {
                    return format!("Unable to find the user id. Only saw {:?}", user_option)
                        .to_string()
                }
            },
            None => {
                return "Looks like no user id specified.".to_string();
            }
        };

        let account: GameId = match options.iter().find(|o| o.name == "summoner") {
            Some(account_option) => match account_option.value.as_ref().unwrap().as_str() {
                Some(name) => {
                    let text = name.trim();
                    if text.is_empty() {
                        return "No name specified".to_string();
                    }
                    if text.contains("#") {
                        let mut parts = text.split("#");
                        GameId::RiotId(
                            parts.next().unwrap().to_string(),
                            parts.next().unwrap().to_string(),
                        )
                    } else {
                        GameId::SummonerName(text.to_string())
                    }
                }
                None => return "No name specified".to_string(),
            },
            None => return "No name specified".to_string(),
        };

        let member_info = match &account {
            GameId::RiotId(name, tag) => {
                let account = riot_api
                    .account_v1()
                    .get_by_riot_id(riven::consts::RegionalRoute::AMERICAS, &name, &tag)
                    .await
                    .unwrap();
                match account {
                    Some(account) => riot_api
                        .summoner_v4()
                        .get_by_puuid(riven::consts::PlatformRoute::NA1, &account.puuid)
                        .await
                        .unwrap(),
                    None => {
                        return "Unable to find the matching account".to_string();
                    }
                }
            }
            GameId::SummonerName(name) => riot_api
                .summoner_v4()
                .get_by_summoner_name(riven::consts::PlatformRoute::NA1, &name)
                .await
                .unwrap()
                .unwrap(),
        };

        let connection = rusqlite::Connection::open("sqlite.db").unwrap();
        let mut statement = connection
            .prepare("INSERT INTO User (DiscordUsername, SummonerName, DiscordId, DiscordDisplayName) VALUES (?1, ?2, ?3, ?4);").unwrap();
        statement
            .execute([
                &user.name,
                &match &account {
                    GameId::RiotId(name, tag) => format!("{}#{}", name, tag),
                    GameId::SummonerName(name) => name.to_string(),
                },
                &user.id.as_u64().to_string(),
                &user.name,
            ])
            .unwrap();
        let mut statement = connection
            .prepare("INSERT INTO Summoner (Id, AccountId, Puuid, Name) VALUES (?1, ?2, ?3, ?4);")
            .unwrap();
        statement
            .execute([
                member_info.id,
                member_info.account_id,
                member_info.puuid,
                match &account {
                    GameId::RiotId(name, tag) => format!("{}#{}", name, tag),
                    GameId::SummonerName(name) => name.to_string(),
                },
            ])
            .unwrap();
        return "".to_string();
    }
}

pub mod clash {
    use anyhow::{anyhow, Result};
    use riven::RiotApi;
    use serenity::builder;
    use serenity::model::prelude::application_command::CommandDataOption;
    use serenity::model::prelude::command::CommandOptionType;
    use serenity::prelude::Context;
    use std::env;

    use super::GameId;

    pub fn register(
        command: &mut builder::CreateApplicationCommand,
    ) -> &mut builder::CreateApplicationCommand {
        command
            .name("clash")
            .description("Looks up clash team info based on a player on the enemy team.")
            .create_option(|option| {
                option
                    .name("summoner")
                    .description("The summoner name / riot id.")
                    .kind(CommandOptionType::String)
                    .required(true)
            })
    }

    pub async fn run(_ctx: &Context, options: &[CommandDataOption]) -> Result<String> {
        let riot_api_key: &str = &env::var("RIOT_API_TOKEN").unwrap();
        let riot_api = RiotApi::new(riot_api_key);

        let account: GameId = match options.iter().find(|o| o.name == "summoner") {
            Some(account_option) => match account_option.value.as_ref().unwrap().as_str() {
                Some(name) => {
                    let text = name.trim();
                    if text.is_empty() {
                        return Err(anyhow!("No name specified".to_string()));
                    }
                    if text.contains("#") {
                        let mut parts = text.split("#");
                        GameId::RiotId(
                            parts.next().unwrap().to_string(),
                            parts.next().unwrap().to_string(),
                        )
                    } else {
                        GameId::SummonerName(text.to_string())
                    }
                }
                None => return Err(anyhow!("No name specified".to_string())),
            },
            None => return Err(anyhow!("No name specified".to_string())),
        };

        let member_info = match &account {
            GameId::RiotId(name, tag) => {
                let account = riot_api
                    .account_v1()
                    .get_by_riot_id(riven::consts::RegionalRoute::AMERICAS, &name, &tag)
                    .await
                    .unwrap();
                match account {
                    Some(account) => riot_api
                        .summoner_v4()
                        .get_by_puuid(riven::consts::PlatformRoute::NA1, &account.puuid)
                        .await
                        .unwrap(),
                    None => {
                        return Err(anyhow!("Unable to find the matching account".to_string()));
                    }
                }
            }
            GameId::SummonerName(name) => riot_api
                .summoner_v4()
                .get_by_summoner_name(riven::consts::PlatformRoute::NA1, &name)
                .await
                .unwrap()
                .unwrap(),
        };

        let clash_players = riot_api
            .clash_v1()
            .get_players_by_summoner(riven::consts::PlatformRoute::NA1, &member_info.id)
            .await?;
        let clash_tournaments = riot_api
            .clash_v1()
            .get_tournaments(riven::consts::PlatformRoute::NA1)
            .await?;
        clash_tournaments.iter().filter(|t| {
            t.schedule
                .iter()
                .any(|phase| phase.start_time <= chrono::Local::now().timestamp_micros())
        });
        let team_ids = clash_players
            .iter()
            .filter(|p| p.team_id.is_some())
            .map(|player| player.team_id.as_ref().unwrap())
            .map(|team_id| async {
                let team_fetch = riot_api
                    .clash_v1()
                    .get_team_by_id(riven::consts::PlatformRoute::NA1, team_id)
                    .await
                    .unwrap();
            });

        Ok("".to_string())
    }
}

#[cfg(test)]
mod tests {

    use rand::{rngs::SmallRng, SeedableRng};
    use serenity::model::prelude::Member;

    use crate::commands::groups::generate_groups;

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
        let value = generate_groups(
            vec![member(123, "Test User"), member(234, "Test User 2")],
            2,
            &mut rng,
        );
        assert_eq!("Group 1:\n\n<@234>\n\nGroup 2:\n\n<@123>", value);
    }

    #[test]
    fn splits_three_users_into_two_groups() {
        let mut rng = SmallRng::seed_from_u64(528);
        let value = generate_groups(
            vec![
                member(123, "Test User"),
                member(234, "Test User 2"),
                member(345, "Test User 3"),
            ],
            2,
            &mut rng,
        );
        assert_eq!("Group 1:\n\n<@234> <@345>\n\nGroup 2:\n\n<@123>", value);
    }

    #[test]
    fn warns_if_too_many_groups() {
        let mut rng = SmallRng::seed_from_u64(528);
        let value = generate_groups(
            vec![
                member(123, "Test User"),
                member(234, "Test User 2"),
                member(345, "Test User 3"),
            ],
            4,
            &mut rng,
        );
        assert_eq!("Too many groups requested, and not enough users.", value);
    }

    #[test]
    fn splits_into_more_than_two_groups() {
        let mut rng = SmallRng::seed_from_u64(528);
        let value = generate_groups(
            vec![
                member(123, "Test User"),
                member(234, "Test User 2"),
                member(345, "Test User 3"),
            ],
            3,
            &mut rng,
        );
        assert_eq!(
            "Group 1:\n\n<@234>\n\nGroup 2:\n\n<@345>\n\nGroup 3:\n\n<@123>",
            value
        );
    }

    #[test]
    fn splitting_20_players_into_4_groups() {
        let mut rng = SmallRng::seed_from_u64(528);
        let value = generate_groups(
            (1..21)
                .map(|i| member(i, &format!("Test User {}", i)))
                .collect::<Vec<_>>(),
            4,
            &mut rng,
        );
        assert_eq!("Group 1:\n\n<@9> <@8> <@7> <@6> <@12>\n\nGroup 2:\n\n<@2> <@14> <@1> <@19> <@10>\n\nGroup 3:\n\n<@13> <@5> <@17> <@16> <@18>\n\nGroup 4:\n\n<@11> <@15> <@20> <@4> <@3>", value);
    }
}
