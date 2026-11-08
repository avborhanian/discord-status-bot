use std::io::{self, IsTerminal, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use lcu_relay::lcu::LockfileLcu;
use lcu_relay::{RelayConfig, RequestHandler, run_forever};
use serde::{Deserialize, Serialize};

const USAGE: &str = "\
lcu_relay: lets discord_status_bot read League match history through this PC's League client.

USAGE:
    lcu_relay [OPTIONS]

OPTIONS:
    --server <url>        WebSocket URL (ws(s)://host:port/relay/ws)
    --secret <secret>     Shared authentication secret
    --name <name>         Relay identity name (defaults to computer name)
    --lockfile <path>     Path to League Client lockfile
    --rate-limit <n>      Max requests per minute (default: 120)
    --config <path>       Custom path to config.json
    --config-path         Print the default configuration file location and exit
    --save                Save current arguments to the configuration file
    --reset               Clear saved configuration and re-prompt
    -h, --help            Print help

CONFIGURATION ORDER:
    1. CLI flags (highest priority)
    2. Environment variables / .env (RELAY_SERVER, RELAY_SECRET, etc.)
    3. Saved configuration file (%APPDATA%\\lcu_relay\\config.json)
    4. First-run interactive setup (when run without config in a terminal)
";

const DEFAULT_LOCKFILE: &str = r"C:\Riot Games\League of Legends\lockfile";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PersistentConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lockfile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limit: Option<u32>,
}

fn default_config_path() -> Option<PathBuf> {
    #[cfg(windows)]
    {
        if let Ok(appdata) = std::env::var("APPDATA") {
            return Some(PathBuf::from(appdata).join("lcu_relay").join("config.json"));
        }
        if let Ok(userprofile) = std::env::var("USERPROFILE") {
            return Some(
                PathBuf::from(userprofile)
                    .join("AppData")
                    .join("Roaming")
                    .join("lcu_relay")
                    .join("config.json"),
            );
        }
    }
    #[cfg(not(windows))]
    {
        if let Ok(xdg) = std::env::var("XDG_CONFIG_HOME") {
            return Some(PathBuf::from(xdg).join("lcu_relay").join("config.json"));
        }
        if let Ok(home) = std::env::var("HOME") {
            return Some(
                PathBuf::from(home)
                    .join(".config")
                    .join("lcu_relay")
                    .join("config.json"),
            );
        }
    }
    None
}

fn load_env_files() {
    // 1. Current working directory or parent directories
    let _ = dotenvy::dotenv();

    // 2. Directory containing the executable (e.g. if run from a shortcut or another working dir)
    if let Ok(exe_path) = std::env::current_exe() {
        if let Some(exe_dir) = exe_path.parent() {
            let _ = dotenvy::from_path(exe_dir.join(".env"));
            let _ = dotenvy::from_path(exe_dir.join("lcu_relay.env"));
        }
    }
}

fn load_persistent_config(path: &Path) -> Option<PersistentConfig> {
    if !path.exists() {
        return None;
    }
    match std::fs::read_to_string(path) {
        Ok(contents) => match serde_json::from_str::<PersistentConfig>(&contents) {
            Ok(cfg) => Some(cfg),
            Err(e) => {
                eprintln!("Warning: failed to parse config at {}: {e}", path.display());
                None
            }
        },
        Err(e) => {
            eprintln!("Warning: failed to read config at {}: {e}", path.display());
            None
        }
    }
}

fn save_persistent_config(path: &Path, config: &PersistentConfig) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create config directory at {}", parent.display()))?;
    }
    let json = serde_json::to_string_pretty(config)?;
    std::fs::write(path, json)
        .with_context(|| format!("failed to write config to {}", path.display()))?;
    Ok(())
}

fn prompt_for_config(config_path: Option<&Path>) -> Result<(String, String, Option<String>)> {
    println!();
    println!("=== lcu_relay First-Time Setup ===");
    println!("No configuration found. Let's configure lcu_relay for this PC:");
    println!();

    print!("1. Relay server WebSocket URL (e.g. ws://192.168.1.50:8787/relay/ws):\n> ");
    io::stdout().flush()?;
    let mut server = String::new();
    io::stdin().read_line(&mut server)?;
    let server = server.trim().to_string();
    if server.is_empty() {
        bail!("Server URL cannot be empty");
    }
    if !(server.starts_with("ws://") || server.starts_with("wss://")) {
        bail!("Server URL must start with ws:// or wss://");
    }

    print!("\n2. Relay secret (RELAY_SECRET from bot's .env):\n> ");
    io::stdout().flush()?;
    let mut secret = String::new();
    io::stdin().read_line(&mut secret)?;
    let secret = secret.trim().to_string();
    if secret.is_empty() {
        bail!("Relay secret cannot be empty");
    }

    let default_name = std::env::var("COMPUTERNAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| "relay".to_string());
    print!("\n3. Relay display name (press Enter to use '{default_name}'):\n> ");
    io::stdout().flush()?;
    let mut name = String::new();
    io::stdin().read_line(&mut name)?;
    let name = name.trim();
    let name = if name.is_empty() {
        default_name
    } else {
        name.to_string()
    };

    if let Some(path) = config_path {
        let persistent = PersistentConfig {
            server: Some(server.clone()),
            secret: Some(secret.clone()),
            name: Some(name.clone()),
            lockfile: None,
            rate_limit: None,
        };
        if let Err(e) = save_persistent_config(path, &persistent) {
            eprintln!("Warning: failed to save configuration to {}: {e}", path.display());
        } else {
            println!("\nConfiguration successfully saved to: {}", path.display());
            println!("You won't need to enter these arguments next time!\n");
        }
    }

    Ok((server, secret, Some(name)))
}

fn parse_args() -> Result<(RelayConfig, PathBuf, u32)> {
    load_env_files();

    let mut custom_config_path: Option<PathBuf> = None;
    let mut explicit_server: Option<String> = None;
    let mut explicit_secret: Option<String> = None;
    let mut explicit_name: Option<String> = None;
    let mut explicit_lockfile: Option<String> = None;
    let mut explicit_rate_limit: Option<u32> = None;
    let mut save_requested = false;
    let mut reset_requested = false;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = || args.next().with_context(|| format!("{arg} needs a value"));
        match arg.as_str() {
            "--server" => explicit_server = Some(value()?),
            "--secret" => explicit_secret = Some(value()?),
            "--name" => explicit_name = Some(value()?),
            "--lockfile" => explicit_lockfile = Some(value()?),
            "--rate-limit" => explicit_rate_limit = Some(value()?.parse().context("invalid --rate-limit")?),
            "--config" => custom_config_path = Some(PathBuf::from(value()?)),
            "--config-path" => {
                let p = default_config_path().unwrap_or_else(|| PathBuf::from("config.json"));
                println!("{}", p.display());
                std::process::exit(0);
            }
            "--save" => save_requested = true,
            "--reset" => reset_requested = true,
            "-h" | "--help" => {
                print!("{USAGE}");
                std::process::exit(0);
            }
            other => bail!("unknown argument {other}\n\n{USAGE}"),
        }
    }

    let config_file_path = custom_config_path.or_else(default_config_path);

    if reset_requested {
        if let Some(ref path) = config_file_path {
            if path.exists() {
                let _ = std::fs::remove_file(path);
                println!("Cleared configuration at {}", path.display());
            }
        }
    }

    // Load persistent config if available
    let stored = if reset_requested {
        None
    } else {
        config_file_path.as_deref().and_then(load_persistent_config)
    };

    // Priority:
    // 1. Explicit CLI args
    // 2. Env vars (or .env)
    // 3. Stored config file
    let mut server = explicit_server
        .or_else(|| std::env::var("RELAY_SERVER").ok())
        .or_else(|| stored.as_ref().and_then(|c| c.server.clone()));

    let mut secret = explicit_secret
        .or_else(|| std::env::var("RELAY_SECRET").ok())
        .or_else(|| stored.as_ref().and_then(|c| c.secret.clone()));

    let mut name = explicit_name
        .or_else(|| std::env::var("RELAY_NAME").ok())
        .or_else(|| stored.as_ref().and_then(|c| c.name.clone()));

    let lockfile = explicit_lockfile
        .or_else(|| std::env::var("LCU_LOCKFILE").ok())
        .or_else(|| stored.as_ref().and_then(|c| c.lockfile.clone()));

    let rate_limit = explicit_rate_limit
        .or_else(|| std::env::var("RELAY_RATE_LIMIT").ok().and_then(|r| r.parse().ok()))
        .or_else(|| stored.as_ref().and_then(|c| c.rate_limit))
        .unwrap_or(lcu_relay::REQUESTS_PER_MINUTE);

    // If still missing server or secret, try interactive prompt if in a terminal
    if server.is_none() || secret.is_none() {
        if io::stdin().is_terminal() && io::stdout().is_terminal() {
            let (p_server, p_secret, p_name) = prompt_for_config(config_file_path.as_deref())?;
            server = Some(p_server);
            secret = Some(p_secret);
            if name.is_none() {
                name = p_name;
            }
        } else {
            let config_hint = config_file_path
                .as_ref()
                .map(|p| format!("\n  - Config file: {}", p.display()))
                .unwrap_or_default();
            bail!(
                "missing --server and/or --secret\n\n\
                You can configure lcu_relay via:\n\
                - Saved config file: {config_hint}\n\
                - Environment variables or .env file: RELAY_SERVER, RELAY_SECRET\n\
                - CLI flags: lcu_relay --server <url> --secret <key>\n\n\
                {USAGE}"
            );
        }
    }

    let Some(server) = server else { bail!("missing server URL") };
    let Some(secret) = secret else { bail!("missing relay secret") };

    if !(server.starts_with("ws://") || server.starts_with("wss://")) {
        bail!("--server must start with ws:// or wss://");
    }

    let name = name
        .or_else(|| std::env::var("COMPUTERNAME").ok())
        .or_else(|| std::env::var("HOSTNAME").ok())
        .unwrap_or_else(|| "relay".to_string());

    let lockfile = PathBuf::from(lockfile.unwrap_or_else(|| DEFAULT_LOCKFILE.to_string()));

    // If user passed --save, save the current configuration
    if save_requested {
        if let Some(ref path) = config_file_path {
            let persistent = PersistentConfig {
                server: Some(server.clone()),
                secret: Some(secret.clone()),
                name: Some(name.clone()),
                lockfile: Some(lockfile.to_string_lossy().to_string()),
                rate_limit: Some(rate_limit),
            };
            save_persistent_config(path, &persistent)?;
            println!("Saved configuration to {}", path.display());
        }
    }

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
