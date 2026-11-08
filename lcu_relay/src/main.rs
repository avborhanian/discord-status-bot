use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use lcu_relay::lcu::LockfileLcu;
use lcu_relay::{RelayConfig, RequestHandler, run_forever};

const USAGE: &str = "\
lcu_relay: lets discord_status_bot read League match history through this PC's League client.

USAGE:
    lcu_relay --server <ws(s)://host:port/relay/ws> --secret <secret> [--name <name>] [--lockfile <path>] [--rate-limit <n>]

Each option can also be set with an environment variable:
    RELAY_SERVER, RELAY_SECRET, RELAY_NAME, LCU_LOCKFILE, RELAY_RATE_LIMIT
";

const DEFAULT_LOCKFILE: &str = r"C:\Riot Games\League of Legends\lockfile";

fn parse_args() -> Result<(RelayConfig, PathBuf, u32)> {
    let mut server = std::env::var("RELAY_SERVER").ok();
    let mut secret = std::env::var("RELAY_SECRET").ok();
    let mut name = std::env::var("RELAY_NAME").ok();
    let mut lockfile = std::env::var("LCU_LOCKFILE").ok();
    let mut rate_limit: u32 = std::env::var("RELAY_RATE_LIMIT")
        .ok()
        .and_then(|r| r.parse().ok())
        .unwrap_or(lcu_relay::REQUESTS_PER_MINUTE);

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = || args.next().with_context(|| format!("{arg} needs a value"));
        match arg.as_str() {
            "--server" => server = Some(value()?),
            "--secret" => secret = Some(value()?),
            "--name" => name = Some(value()?),
            "--lockfile" => lockfile = Some(value()?),
            "--rate-limit" => rate_limit = value()?.parse().context("invalid --rate-limit")?,
            "-h" | "--help" => {
                print!("{USAGE}");
                std::process::exit(0);
            }
            other => bail!("unknown argument {other}\n\n{USAGE}"),
        }
    }

    let Some(server) = server else { bail!("missing --server\n\n{USAGE}") };
    let Some(secret) = secret else { bail!("missing --secret\n\n{USAGE}") };
    if !(server.starts_with("ws://") || server.starts_with("wss://")) {
        bail!("--server must start with ws:// or wss://");
    }
    let name = name
        .or_else(|| std::env::var("COMPUTERNAME").ok())
        .unwrap_or_else(|| "relay".to_string());
    let lockfile = PathBuf::from(lockfile.unwrap_or_else(|| DEFAULT_LOCKFILE.to_string()));
    Ok((RelayConfig { server, secret, name }, lockfile, rate_limit))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_target(false).init();
    let (config, lockfile, rate_limit) = parse_args()?;
    tracing::info!(
        "lcu_relay {} starting as '{}' (rate limit: {} req/min, lockfile: {})",
        lcu_relay::VERSION,
        config.name,
        rate_limit,
        lockfile.display()
    );
    let handler = RequestHandler::with_rate_limit(LockfileLcu::new(lockfile)?, rate_limit);
    run_forever(config, handler).await;
    Ok(())
}
