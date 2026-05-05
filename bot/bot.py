#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import json
import os
import socket
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import discord
from discord import app_commands
from discord.ext import tasks


DEFAULT_BRIDGE_DIR = Path.home() / ".local" / "share" / "nicotine" / "discord-bridge"
DEFAULT_RUNTIME_PATH = DEFAULT_BRIDGE_DIR / "runtime.json"


def read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def load_runtime() -> dict[str, Any]:
    runtime_path = Path(os.environ.get("NICOTINE_BRIDGE_CONFIG") or DEFAULT_RUNTIME_PATH).expanduser()
    return read_json(runtime_path)


def load_env_file(path: Path):
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


RUNTIME = load_runtime()
ENV_PATH = Path(RUNTIME.get("bot_env_path") or (Path.home() / "nicotine-discord-bridge" / ".env")).expanduser()
load_env_file(ENV_PATH)
BRIDGE_SOCKET = Path(os.environ.get("NICOTINE_BRIDGE_SOCKET") or RUNTIME.get("socket_path") or (DEFAULT_BRIDGE_DIR / "control.sock")).expanduser()
EVENTS_FILE = Path(os.environ.get("NICOTINE_BRIDGE_EVENTS") or RUNTIME.get("events_path") or (DEFAULT_BRIDGE_DIR / "events.jsonl")).expanduser()
STATE_FILE = Path(os.environ.get("NICOTINE_BRIDGE_STATE") or (Path(RUNTIME.get("data_dir") or DEFAULT_BRIDGE_DIR) / "bot-state.json")).expanduser()
RESULTS_LIMIT = max(1, min(25, int(RUNTIME.get("results_limit", 5) or 5)))
TRACK_PICKER_LIMIT = max(1, min(25, int(RUNTIME.get("track_picker_limit", 25) or 25)))
DOWNLOAD_ALERT_MODE = str(RUNTIME.get("download_alert_mode", "file") or "file").strip().lower()
UPLOAD_ALERT_MODE = str(RUNTIME.get("upload_alert_mode", "file") or "file").strip().lower()
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN", "").strip()
GUILD_ID = os.environ.get("DISCORD_GUILD_ID", "").strip()
ALERT_CHANNEL_ID = os.environ.get("DISCORD_ALERT_CHANNEL_ID", "").strip()


@dataclass
class PendingRequest:
    channel_id: int
    user_id: int
    user_name: str
    query: str


class BridgeState:
    def __init__(self, path: Path):
        self.path = path
        self.cursor = 0
        self.pending: dict[str, PendingRequest] = {}
        self.load()

    def load(self):
        data = read_json(self.path)
        self.cursor = int(data.get("cursor", 0) or 0)
        self.pending = {}
        for request_id, item in (data.get("pending") or {}).items():
            try:
                self.pending[request_id] = PendingRequest(
                    channel_id=int(item["channel_id"]),
                    user_id=int(item["user_id"]),
                    user_name=str(item["user_name"]),
                    query=str(item["query"]),
                )
            except Exception:
                pass

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(
                {
                    "cursor": self.cursor,
                    "pending": {
                        request_id: {
                            "channel_id": item.channel_id,
                            "user_id": item.user_id,
                            "user_name": item.user_name,
                            "query": item.query,
                        }
                        for request_id, item in self.pending.items()
                    },
                },
                indent=2,
            ),
            encoding="utf-8",
        )


state = BridgeState(STATE_FILE)
client = discord.Client(intents=discord.Intents.default())
tree = app_commands.CommandTree(client)
guild_obj = discord.Object(id=int(GUILD_ID)) if GUILD_ID else None


async def bridge_call(payload: dict[str, Any]) -> dict[str, Any]:
    if not BRIDGE_SOCKET.exists():
        return {"ok": False, "error": f"bridge socket not found: {BRIDGE_SOCKET}"}

    def run_call():
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.connect(str(BRIDGE_SOCKET))
            sock.sendall((json.dumps(payload) + "\n").encode("utf-8"))
            return json.loads(sock.recv(65536).decode("utf-8").strip())

    return await asyncio.to_thread(run_call)


async def poll_bridge(op: str, request_id: str, *, ready_when, timeout: float, interval: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last = {"ok": False, "error": "timed out"}
    while time.monotonic() < deadline:
        last = await bridge_call({"op": op, "request_id": request_id})
        if ready_when(last):
            return last
        await asyncio.sleep(interval)
    return last


def register_pending(request_ids: list[str], interaction: discord.Interaction, query: str):
    for request_id in request_ids:
        state.pending[request_id] = PendingRequest(
            channel_id=interaction.channel_id or interaction.user.id,
            user_id=interaction.user.id,
            user_name=str(interaction.user),
            query=query,
        )
    state.save()


DISCORD_CONTENT_LIMIT = 2000


def trim(text: str, limit: int) -> str:
    text = str(text or "")
    return text if len(text) <= limit else text[: max(0, limit - 1)] + "…"


def fit_discord_content(text: str) -> str:
    return trim(text, DISCORD_CONTENT_LIMIT)


async def safe_edit(interaction: discord.Interaction, *, content: str, view=None):
    await interaction.edit_original_response(content=fit_discord_content(content), view=view)


async def safe_send(interaction: discord.Interaction, *, content: str, ephemeral: bool = True):
    await interaction.response.send_message(fit_discord_content(content), ephemeral=ephemeral)


def folder_name(path: str) -> str:
    return (path or "").split("\\")[-1] or "<root>"


def folder_parts(path: str) -> list[str]:
    return [part for part in str(path or "").split("\\") if part]


def artist_album_text(result: dict[str, Any]) -> str:
    artist = str(result.get("artist") or "").strip()
    album = str(result.get("album") or folder_name(result.get("folder", ""))).strip() or "<root>"
    if artist and artist.lower() != album.lower():
        return f"{artist} — {album}"
    return album


def result_file_count(result: dict[str, Any]) -> int:
    return int(result.get("display_file_count") or result.get("audio_file_count") or result.get("match_count") or 0)


def result_label(result: dict[str, Any], index: int | None = None) -> str:
    prefix = f"{index}. " if index is not None else ""
    return trim(f"{prefix}{artist_album_text(result)}", 100)


def result_desc(result: dict[str, Any]) -> str:
    parts = [f"{result_file_count(result)} files"]
    format_summary = str(result.get("format_summary") or "").strip()
    if format_summary:
        parts.append(format_summary)
    return trim(" • ".join(parts), 100)


def download_file_label(path: str) -> str:
    parts = folder_parts(path)
    return trim(parts[-1] if parts else (path or "file"), 120)


def download_album_label(path: str) -> str:
    parts = folder_parts(path)
    if len(parts) >= 2:
        album = parts[-2]
        if len(parts) >= 3:
            artist = parts[-3]
            if artist and artist.lower() != album.lower():
                return trim(f"{artist} — {album}", 120)
        return trim(album, 120)
    return download_file_label(path)


def alert_path_label(path: str, mode: str) -> str:
    return download_album_label(path) if mode == "album" else download_file_label(path)


def track_label(item: dict[str, Any]) -> str:
    return trim(item.get("name") or item.get("fullpath") or "file", 100)


def track_desc(item: dict[str, Any]) -> str:
    return trim(item.get("size_human") or "", 100)


async def get_channel(channel_id: int | None):
    if not channel_id:
        return None
    channel = client.get_channel(channel_id)
    if channel is not None:
        return channel
    try:
        return await client.fetch_channel(channel_id)
    except Exception:
        return None


async def resolve_alert_channel():
    if ALERT_CHANNEL_ID:
        try:
            channel = await get_channel(int(ALERT_CHANNEL_ID))
        except ValueError:
            channel = None
        if channel is not None:
            return channel
    guild = client.get_guild(int(GUILD_ID)) if GUILD_ID else None
    if guild is None and client.guilds:
        guild = client.guilds[0]
    if guild is None:
        return None
    return guild.system_channel or (guild.text_channels[0] if guild.text_channels else None)


async def send_message(channel_id: int, text: str) -> bool:
    channel = await get_channel(channel_id)
    if channel is None:
        return False
    try:
        await channel.send(fit_discord_content(text))
        return True
    except Exception:
        return False


class SearchResultSelect(discord.ui.Select):
    def __init__(self, results: list[dict[str, Any]]):
        super().__init__(
            placeholder="Pick a folder / album",
            min_values=1,
            max_values=1,
            options=[
                discord.SelectOption(label=result_label(result, index + 1), description=result_desc(result) or None, value=str(index))
                for index, result in enumerate(results)
            ],
        )
        self.results = results

    async def callback(self, interaction: discord.Interaction):
        await self.view.choose_result(interaction, self.results[int(self.values[0])])


class SearchResultsView(discord.ui.View):
    def __init__(self, session: dict[str, Any], results: list[dict[str, Any]]):
        super().__init__(timeout=600)
        self.session = session
        self.add_item(SearchResultSelect(results))

    async def choose_result(self, interaction: discord.Interaction, result: dict[str, Any]):
        await interaction.response.defer(ephemeral=True, thinking=True)
        request_id = str(uuid.uuid4())
        reply = await bridge_call({"op": "browse_folder", "request_id": request_id, "user": result["user"], "folder": result.get("folder", "")})
        if not reply.get("ok"):
            await safe_edit(interaction, content=f"Browse failed: {reply}", view=None)
            return
        browse = await poll_bridge(
            "browse_folder_results",
            request_id,
            ready_when=lambda item: item.get("ok") and item.get("ready"),
            timeout=25.0,
            interval=1.5,
        )
        if not browse.get("ok") or not browse.get("ready"):
            await safe_edit(interaction, content=f"I couldn't load that folder yet: {browse}", view=None)
            return
        files = browse.get("files") or []
        self.session.update({"browse_request_id": request_id, "browse": browse, "folder": browse.get("folder", ""), "files": files})
        text = (
            f"Selected `{artist_album_text(result)}`\n"
            f"Matched audio files: {result_file_count(result)}\n"
            f"Folder tracks found: {len(files)}"
        )
        if len(files) > TRACK_PICKER_LIMIT:
            text += f"\nDiscord will only show the first {TRACK_PICKER_LIMIT} tracks in the picker."
        await safe_edit(interaction, content=text, view=BrowseDecisionView(self.session))


class BrowseDecisionView(discord.ui.View):
    def __init__(self, session: dict[str, Any]):
        super().__init__(timeout=600)
        self.session = session

    @discord.ui.button(label="Download all", style=discord.ButtonStyle.green)
    async def download_all(self, interaction: discord.Interaction, _button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        reply = await bridge_call({"op": "download_folder", "request_id": self.session["browse_request_id"], "dest": ""})
        if not reply.get("ok"):
            await safe_edit(interaction, content=f"Download failed: {reply}", view=None)
            return
        request_ids = reply.get("request_ids", [])
        register_pending(request_ids, interaction, f"{self.session.get('query', 'album')} :: {self.session.get('folder', '<root>')}")
        await safe_edit(interaction, content=f"Queued {reply.get('queued', 0)} file(s) for download.", view=None)

    @discord.ui.button(label="Pick specific tracks", style=discord.ButtonStyle.blurple)
    async def pick_specific(self, interaction: discord.Interaction, _button: discord.ui.Button):
        files = (self.session.get("files") or [])[:TRACK_PICKER_LIMIT]
        if not files:
            await safe_send(interaction, content="No files were loaded for that folder.", ephemeral=True)
            return
        await interaction.response.edit_message(content="Pick the tracks you want to download:", view=TrackSelectView(self.session, files))


class TrackSelect(discord.ui.Select):
    def __init__(self, files: list[dict[str, Any]]):
        super().__init__(
            placeholder="Pick tracks to download",
            min_values=1,
            max_values=min(25, len(files)),
            options=[
                discord.SelectOption(label=track_label(item), description=track_desc(item) or None, value=str(index))
                for index, item in enumerate(files)
            ],
        )
        self.files = files

    async def callback(self, interaction: discord.Interaction):
        chosen = [self.files[int(value)] for value in self.values]
        await self.view.download_selected(interaction, chosen)


class TrackSelectView(discord.ui.View):
    def __init__(self, session: dict[str, Any], files: list[dict[str, Any]]):
        super().__init__(timeout=600)
        self.session = session
        self.add_item(TrackSelect(files))

    async def download_selected(self, interaction: discord.Interaction, chosen: list[dict[str, Any]]):
        await interaction.response.defer(ephemeral=True, thinking=True)
        reply = await bridge_call({
            "op": "download_files",
            "request_id": self.session["browse_request_id"],
            "files": [item["name"] for item in chosen],
            "dest": "",
        })
        if not reply.get("ok"):
            await safe_edit(interaction, content=f"Download failed: {reply}", view=None)
            return
        request_ids = reply.get("request_ids", [])
        register_pending(request_ids, interaction, f"{self.session.get('query', 'album')} :: {self.session.get('folder', '<root>')}")
        await safe_edit(interaction, content=f"Queued {reply.get('queued', 0)} selected track(s).", view=None)


async def sync_slash_commands():
    try:
        if guild_obj is not None:
            tree.copy_global_to(guild=guild_obj)
            synced = await tree.sync(guild=guild_obj)
            print(f"slash sync ok (guild): {len(synced)} commands")
        else:
            synced = await tree.sync()
            print(f"slash sync ok (global): {len(synced)} commands")
    except Exception as exc:
        print(f"slash sync failed: {exc}")


@client.event
async def setup_hook():
    if guild_obj is not None:
        try:
            app_id = client.application_id or (await client.application_info()).id
            await client.http.bulk_upsert_global_commands(app_id, [])
            print("cleared stale global application commands")
        except Exception as exc:
            print(f"failed to clear global commands: {exc}")
    await sync_slash_commands()


@client.event
async def on_ready():
    if not watch_bridge_events.is_running():
        watch_bridge_events.start()
    print(f"Logged in as {client.user} | socket={BRIDGE_SOCKET} | events={EVENTS_FILE} | state={STATE_FILE}")


slsk = app_commands.Group(name="slsk", description="Soulseek / Nicotine bridge commands")


@slsk.command(name="ping", description="Check that the local Nicotine bridge is alive")
async def slsk_ping(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)
    await safe_edit(interaction, content=f"Bridge: {await bridge_call({'op': 'ping'})}")


@slsk.command(name="album", description="Search Soulseek for an album or folder, then choose what to download")
@app_commands.describe(query="Album / artist / folder search text")
async def slsk_album(interaction: discord.Interaction, query: str):
    await interaction.response.defer(ephemeral=True, thinking=True)
    request_id = str(uuid.uuid4())
    reply = await bridge_call({"op": "search", "request_id": request_id, "query": query})
    if not reply.get("ok"):
        await safe_edit(interaction, content=f"Search failed: {reply}", view=None)
        return
    search = await poll_bridge(
        "search_results",
        reply["request_id"],
        ready_when=lambda item: item.get("ok") and bool(item.get("results")),
        timeout=18.0,
        interval=2.0,
    )
    results = search.get("results") or []
    if not search.get("ok") or not results:
        await safe_edit(interaction, content=f"No useful results found for `{query}` yet.", view=None)
        return
    lines = [f"Top {len(results)} results for `{query}`:"]
    for index, result in enumerate(results, start=1):
        line = f"{index}. {result_label(result)} ({result_file_count(result)} files, {result.get('format_summary', 'unknown audio')})"
        lines.append(line)
    lines.append(f"Use the dropdown to choose the folder / album you want. (limit: {RESULTS_LIMIT})")
    await safe_edit(
        interaction,
        content="\n".join(lines),
        view=SearchResultsView({"query": query, "search_request_id": reply["request_id"], "results": results}, results),
    )


@slsk.command(name="download", description="Queue an exact Soulseek username + path download")
@app_commands.describe(user="Soulseek username", path="Virtual path inside the user's share", destination="Optional local destination folder")
async def slsk_download(interaction: discord.Interaction, user: str, path: str, destination: str | None = None):
    reply = await bridge_call({
        "op": "download",
        "request_id": str(uuid.uuid4()),
        "user": user,
        "path": path,
        "dest": destination or "",
    })
    if not reply.get("ok"):
        await safe_send(interaction, content=f"Failed: {reply}", ephemeral=True)
        return
    register_pending(reply.get("request_ids", []), interaction, f"{user} :: {path}")
    await safe_send(interaction, content=f"Queued exact path download for `{user}` -> `{path}`", ephemeral=True)


tree.add_command(slsk)


@tasks.loop(seconds=2.5)
async def watch_bridge_events():
    if not EVENTS_FILE.exists():
        return
    try:
        with EVENTS_FILE.open("r", encoding="utf-8") as handle:
            handle.seek(state.cursor)
            lines = handle.readlines()
            state.cursor = handle.tell()
    except Exception:
        return
    if lines:
        state.save()
    for raw in lines:
        try:
            event = json.loads(raw.strip())
        except Exception:
            continue
        kind = event.get("event")
        request_id = event.get("request_id")
        if kind == "upload_started":
            channel = await resolve_alert_channel()
            if channel is not None:
                user = event.get("user", "unknown")
                path = event.get("path", "")
                label = alert_path_label(str(path or ""), UPLOAD_ALERT_MODE)
                await send_message(channel.id, f"Someone started downloading from you: `{user}` -> `{label}`")
            continue
        if kind == "upload_finished":
            channel = await resolve_alert_channel()
            if channel is not None:
                user = event.get("user", "unknown")
                path = event.get("path", "")
                label = alert_path_label(str(path or ""), UPLOAD_ALERT_MODE)
                await send_message(channel.id, f"Someone finished downloading from you: `{user}` -> `{label}`")
            continue
        if not request_id or request_id not in state.pending:
            continue
        pending = state.pending[request_id]
        path_label = alert_path_label(str(event.get("path") or ""), DOWNLOAD_ALERT_MODE)
        message = None
        if kind == "started":
            message = f"Download started: `{path_label}`"
        elif kind == "finished":
            message = f"Download finished: `{path_label}`"
            state.pending.pop(request_id, None)
            state.save()
        elif kind == "error":
            message = f"Error downloading `{path_label}`: {event.get('error', 'unknown error')}"
            state.pending.pop(request_id, None)
            state.save()
        if message:
            await send_message(pending.channel_id, message)


if __name__ == "__main__":
    if not DISCORD_TOKEN:
        raise SystemExit(f"Set DISCORD_TOKEN in {ENV_PATH} or the environment first.")
    client.run(DISCORD_TOKEN)
