# Nicotine+ Discord Bridge

Discord slash commands that drive **Nicotine+**  on the same machine: search shares, browse a folder, queue downloads, and get upload-started alerts when someone pulls from you.

## Setup

### 1. Install the Nicotine+ plugin

Copy the entire `discord_bridge/` folder into your Nicotine plugins directory:

| OS | Path |
|----|------|
| Linux | `~/.local/share/nicotine/plugins/` |
| macOS | `~/Library/Application Support/nicotine/plugins/` |
| Windows | `%APPDATA%\nicotine\plugins\` |

### 2. Enable the plugin in Nicotine

**Preferences → Plugins → Discord Bridge → enable it → Settings** (adjust limits if needed).

Restart Nicotine (or toggle the plugin off and back on). This writes `runtime.json`, which the bot needs to connect.

### 3. Configure the bot

```bash
cd bot
cp .env.example .env
```

Open `.env` and fill in your values:

| Variable | Required | What it does |
|----------|----------|--------------|
| `DISCORD_TOKEN` | **Yes** | Your bot token from the [Discord Developer Portal](https://discord.com/developers/applications) |
| `DISCORD_GUILD_ID` | No | Guild (server) ID — limits slash command sync to one server |
| `DISCORD_ALERT_CHANNEL_ID` | No | Channel ID for upload alerts |
| `NICOTINE_BRIDGE_CONFIG` | No | Path to `runtime.json` if not in the default location |
| `NICOTINE_BRIDGE_SOCKET` | No | Override the local Unix socket from `runtime.json` |
| `NICOTINE_BRIDGE_EVENTS` / `NICOTINE_BRIDGE_STATE` | No | Override event log and bot state file paths |
| `NICOTINE_BRIDGE_TIMEOUT` | No | Local bridge-call timeout in seconds (default `8`) |

### 4. Run the bot

```bash
pip install discord.py
python bot.py
```

Keep it running alongside Nicotine.

## Slash commands

- `/slsk ping` — check if the bridge is alive
- `/slsk status` — show bridge latency, Soulseek connectivity, and tracked queue health
- `/slsk album` — search, pick a folder, queue downloads
- `/slsk download` — queue a file by Soulseek username + path
- `/slsk queue` — inspect bridge-tracked downloads
- `/slsk unqueue` — remove one or several bridge-tracked downloads

## Nicotine plugin settings

**Preferences → Plugins → Discord Bridge → Settings**

- `results_limit`, `track_picker_limit`
- `emit_upload_started` / `emit_upload_finished`
- `emit_download_started` / `emit_download_finished`
