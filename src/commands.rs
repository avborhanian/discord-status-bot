pub mod groups {
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
        for (_, [user_id]) in re
            .captures_iter(
                options
                    .iter().find(|o| o.name == "ignore")
                    .map_or("", |o| match &o.value {
                        Some(serde_json::Value::String(s)) => s,
                        Some(_) => "",
                        None => todo!(),
                    }),
            )
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

        let mut members_to_group = active_members
            .iter()
            .filter(|member| !ids_to_ignore.contains(member.user.id.as_u64()))
            .collect::<Vec<_>>();

        members_to_group.shuffle(&mut thread_rng());

        info!("List of members is {:?}", members_to_group);
        info!(
            "Index is calculated as {}",
            members_to_group.len().div_ceil(2)
        );

        format!("Group 1 will contain the following users:\n\n{}\n\nGroup 2 will contain the following users:\n\n{}",
        members_to_group[0..members_to_group.len().div_ceil(2)].iter().map(|m| format!("<@{}>", m.user.id.as_u64())).collect::<Vec<_>>().join(" "),
        members_to_group[members_to_group.len().div_ceil(2)..].iter().map(|m| format!("<@{}>", m.user.id.as_u64())).collect::<Vec<_>>().join(" ")
        )
    }
}
