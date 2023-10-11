pub mod groups {
    use anyhow::{anyhow, Result};
    use rand::seq::SliceRandom;
    use rand::thread_rng;
    use regex::Regex;
    use serenity::builder;
    use serenity::model::guild::Member;
    use serenity::model::prelude::application_command::CommandDataOption;
    use serenity::model::prelude::command::CommandOptionType;
    use serenity::prelude::Context;
    use std::{collections::HashSet, vec};
    use tracing::{error, info};

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
            _ => return Result::Err(anyhow!("Unhandled state, please try again later.")),
        })
    }

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
                    .name("number of groups")
                    .description("The amount of groups to create. Can't use with option max users.")
                    .kind(CommandOptionType::Number)
                    .required(false)
            })
            .create_option(|option| {
                option
                    .name("max users")
                    .description("The maximum amount of users in a group. Can't use with option number of groups.")
                    .kind(CommandOptionType::Number)
                    .required(false)
            })
    }

    pub async fn run(ctx: &Context, options: &[CommandDataOption]) -> String {
        info!("fetching channel info now");
        let channel_fetch = ctx.http.get_channel(705937071641985104).await;
        let channel;
        if let Result::Ok(c) = channel_fetch {
            info!("Channel info here: {:?}", c);
            channel = c;
        } else {
            info!("Voice channel missing");
            return "Unable to find the voice channel.".to_string();
        };
        let active_members: Vec<Member> = match channel {
            serenity::model::prelude::Channel::Guild(guild_channel) => {
                info!("Guild channel info: {:?}", guild_channel);
                match guild_channel.members(ctx.cache.as_ref()).await {
                    Result::Ok(m) => {
                        info!("List of members: {:?}", m);
                        m
                    }
                    Result::Err(e) => {
                        error!("Error encountered while fetching members: {:?}", e);
                        vec![]
                    }
                }
            }
            c => {
                info!("Just not a guild channel - was {}", c);
                vec![]
            }
        };
        if active_members.is_empty() {
            info!("Unable to determine who to remove from the groups");
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
            .find(|o| o.name == "number of groups")
            .map_or(None, |o| match &o.value {
                Some(serde_json::Value::Number(n)) => Some(n),
                Some(_) => None,
                None => None,
            });

        let max_users = options
            .iter()
            .find(|o| o.name == "max users")
            .map_or(None, |o| match &o.value {
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
        (&mut members_to_group).shuffle(rng);

        info!("List of members is {:?}", members_to_group);
        info!(
            "Index is calculated as {}",
            members_to_group.len().div_ceil(2)
        );

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
                .into_iter()
                .map(|i| member(i, &format!("Test User {}", i)))
                .collect::<Vec<_>>(),
            4,
            &mut rng,
        );
        assert_eq!("Group 1:\n\n<@9> <@8> <@7> <@6> <@12>\n\nGroup 2:\n\n<@2> <@14> <@1> <@19> <@10>\n\nGroup 3:\n\n<@13> <@5> <@17> <@16> <@18>\n\nGroup 4:\n\n<@11> <@15> <@20> <@4> <@3", value);
    }
}
