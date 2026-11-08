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

### 2. Run Manually
Open PowerShell or Command Prompt in the directory containing `lcu_relay.exe`:

```powershell
.\lcu_relay.exe --server ws://<BOT_IP_OR_DOMAIN>:8787/relay/ws --secret <YOUR_RELAY_SECRET> --name "MyGamingPC"
```

### 3. Run Automatically on Startup (Recommended)
To keep the relay running whenever your PC starts up:
1. Right-click `lcu_relay.exe` and click **Create shortcut**.
2. Right-click the newly created shortcut and select **Properties**.
3. In the **Target** field, add your flags after the executable path:
   ```text
   "C:\path\to\lcu_relay.exe" --server ws://<BOT_IP_OR_DOMAIN>:8787/relay/ws --secret <YOUR_RELAY_SECRET> --name "MyGamingPC"
   ```
4. Press `Win + R`, type `shell:startup`, and press Enter.
5. Move the shortcut into the Windows Startup folder.

The relay will automatically connect to the bot whenever your PC starts. When the League of Legends client is opened, the relay detects the lockfile and reports `lcu: up` to the bot. When League is closed, the relay simply waits quietly.

---

## Command-Line Arguments & Environment Variables

| Flag | Environment Variable | Default | Description |
|---|---|---|---|
| `--server` | `RELAY_SERVER` | *(required)* | WebSocket URL of the bot's relay hub (e.g. `ws://192.168.1.50:8787/relay/ws` or `wss://bot.example.com/relay/ws`) |
| `--secret` | `RELAY_SECRET` | *(required)* | Shared secret token matching the bot's `RELAY_SECRET` |
| `--name` | `RELAY_NAME` | `%COMPUTERNAME%` | Friendly name for this relay instance |
| `--lockfile` | `LCU_LOCKFILE` | `C:\Riot Games\League of Legends\lockfile` | Custom path to the League client `lockfile` if installed in a non-default directory |

---

## Security & Verification

- **Code Isolation**: The relay client code is fully open-source in `lcu_relay/` and has zero external dependencies besides standard Rust networking crates (`tokio`, `tokio-tungstenite`, `reqwest`).
- **No Inbound Listening Ports**: The binary binds zero listening sockets.
- **Fail-Safe**: If the bot is unreachable or goes offline, the relay reconnects automatically with exponential backoff (up to 30 seconds).
