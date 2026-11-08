# LCU Relay

`lcu_relay` is a lightweight, read-only Windows companion client that connects your local League of Legends Client (LCU) to the `discord_status_bot` running on a remote server or Raspberry Pi.

It allows the bot to track and score special game modes—such as **ARAM: Mayhem (`queueId: 2400`)**—where Riot's public `match-v5` API returns `403 Forbidden`.

---

## How It Works

1. **Outbound-Only WebSocket Connection**: `lcu_relay` opens a secure WebSocket connection to the bot (`ws://<host>:8787/relay/ws` or `wss://...`). You **do not** need to open any ports or configure port-forwarding on your router.
2. **Read-Only & Strict Allowlist**: Only three read-only LCU GET endpoints are ever forwarded:
   - `/lol-summoner/v1/alias/lookup?gameName=...&tagLine=...`
   - `/lol-match-history/v1/products/lol/{puuid}/matches?begIndex=0&endIndex=5`
   - `/lol-match-history/v1/games/{gameId}`
   All other endpoints, mutations (POST/PUT/DELETE), path traversals, or unknown query parameters are blocked locally before reaching your League client.
3. **Local Rate Limiting**: Requests from the bot are throttled to a maximum of 30 requests per minute with token-bucket rate limiting.
4. **Privacy**: Authentication tokens, session cookies, and sensitive identifiers are masked in log output. No private personal data or passwords leave your computer.

---

## Quick Start (For Friends)

### 1. Download `lcu_relay.exe`
Download `lcu_relay.exe` from the latest GitHub Actions build artifact or your bot administrator.

### 2. Run Once to Configure
Simply double-click `lcu_relay.exe` or run it in a terminal without any arguments:

```powershell
.\lcu_relay.exe
```

On first run, it will interactively prompt you for:
1. **Relay server URL**: `ws://<BOT_IP_OR_DOMAIN>:8787/relay/ws` (or `wss://...`)
2. **Relay secret**: The shared secret from the bot administrator
3. **Relay display name**: (Optional, press Enter to default to your computer's name)

Your configuration is automatically saved to `%APPDATA%\lcu_relay\config.json` so you never have to type it again!

Alternatively, you can provide a `.env` file next to `lcu_relay.exe` or pass standard command-line flags.

### 3. Run Automatically on Startup (Recommended)
To keep the relay running whenever your PC starts up:
1. Right-click `lcu_relay.exe` and select **Create shortcut**.
2. Press `Win + R`, type `shell:startup`, and press Enter.
3. Move the shortcut into the Windows Startup folder.

Because your settings are saved in AppData (or `.env`), the shortcut does not need any arguments!

The relay will automatically connect to the bot whenever your PC starts. When the League of Legends client is opened, the relay detects the lockfile and reports `lcu: up` to the bot. When League is closed, the relay simply waits quietly.

---

## Configuration Methods

`lcu_relay` resolves configuration in the following order:

1. **Command-Line Arguments** (highest priority, overrides all)
2. **Environment Variables or `.env` file** (loaded from current working directory or beside `lcu_relay.exe`)
3. **Saved Config File** (`%APPDATA%\lcu_relay\config.json` on Windows, `~/.config/lcu_relay/config.json` on Linux)
4. **Interactive Setup Prompt** (when run without config in an interactive terminal)

### Command-Line Arguments & Environment Variables

| Flag | Environment Variable | Default | Description |
|---|---|---|---|
| `--server` | `RELAY_SERVER` | *(required)* | WebSocket URL of the bot's relay hub (e.g. `ws://192.168.1.50:8787/relay/ws` or `wss://bot.example.com/relay/ws`) |
| `--secret` | `RELAY_SECRET` | *(required)* | Shared secret token matching the bot's `RELAY_SECRET` |
| `--name` | `RELAY_NAME` | `%COMPUTERNAME%` | Friendly name for this relay instance |
| `--lockfile` | `LCU_LOCKFILE` | `C:\Riot Games\League of Legends\lockfile` | Custom path to the League client `lockfile` if installed in a non-default directory |
| `--rate-limit` | `RELAY_RATE_LIMIT` | `120` | Maximum requests forwarded per minute to prevent client flooding |
| `--config <path>` | — | `%APPDATA%\lcu_relay\config.json` | Path to a custom `config.json` |
| `--config-path` | — | — | Print the default configuration file location and exit |
| `--save` | — | — | Save current arguments to the persistent config file |
| `--reset` | — | — | Clear the saved configuration and re-prompt |

---

## Security & Verification

- **Code Isolation**: The relay client code is fully open-source in `lcu_relay/` and has zero external dependencies besides standard Rust networking crates (`tokio`, `tokio-tungstenite`, `reqwest`).
- **No Inbound Listening Ports**: The binary binds zero listening sockets.
- **Fail-Safe**: If the bot is unreachable or goes offline, the relay reconnects automatically with exponential backoff (up to 30 seconds).
