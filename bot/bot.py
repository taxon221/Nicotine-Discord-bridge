#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import time
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import discord
from discord import app_commands
from discord.ext import tasks

from bridge_state import BridgeState, DownloadBatch, DownloadItem, PendingRequest, UploadAlert, heal_downloads_present_on_disk, read_event_lines, read_json
from bridge_transport import unix_json_call
from rendering import (
    RESULT_PAGE_SIZE,
    artist_album_text,
    batch_completed_count,
    download_album_label,
    download_file_label,
    fit_discord_content,
    render_batch_message,
    render_queue_entries,
    render_search_results_page,
    render_upload_alert,
    result_desc,
    result_file_count,
    result_label,
    trim,
    upload_alert_key,
)


DEFAULT_BRIDGE_DIR = Path.home() / ".local" / "share" / "nicotine" / "discord-bridge"
DEFAULT_RUNTIME_PATH = DEFAULT_BRIDGE_DIR / "runtime.json"


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
TRACK_PICKER_LIMIT = max(1, min(25, int(RUNTIME.get("track_picker_limit", 25) or 25)))
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN", "").strip()
GUILD_ID = os.environ.get("DISCORD_GUILD_ID", "").strip()
ALERT_CHANNEL_ID = os.environ.get("DISCORD_ALERT_CHANNEL_ID", "").strip()


state = BridgeState(STATE_FILE)
client = discord.Client(intents=discord.Intents.default())
tree = app_commands.CommandTree(client)
guild_obj = discord.Object(id=int(GUILD_ID)) if GUILD_ID else None
BATCH_EDIT_COOLDOWN_SECONDS = 8.0
SEARCH_RESULTS_FETCH_LIMIT = 100
BROWSE_RETRY_DELAY_SECONDS = 300.0
RECONCILE_INTERVAL_SECONDS = 60.0
BRIDGE_CALL_TIMEOUT_SECONDS = max(1.0, float(os.environ.get("NICOTINE_BRIDGE_TIMEOUT", "8") or 8))
SEARCH_POLL_INTERVAL_SECONDS = 2.0
SEARCH_EMPTY_GRACE_SECONDS = 35.0
SEARCH_SLOW_RETRY_SECONDS = 90.0
UNSHARED_ERROR_MARKERS = ("file not shared", "not shared", "banned")
TERMINAL_QUEUE_STATUSES = {"finished", "cancelled", "canceled", "aborted", "failed", "error", "file not shared", "not shared", "removed"}
batch_last_edit_at: dict[str, float] = {}
dirty_batches: set[str] = set()
upload_alerts: dict[str, UploadAlert] = {}
upload_alert_last_edit_at: dict[str, float] = {}
dirty_upload_alerts: set[str] = set()
last_reconcile_at = 0.0
announcement_views_registered = False


async def bridge_call(payload: dict[str, Any]) -> dict[str, Any]:
    return await asyncio.to_thread(
        unix_json_call,
        BRIDGE_SOCKET,
        payload,
        timeout=BRIDGE_CALL_TIMEOUT_SECONDS,
    )


async def poll_bridge(op: str, request_id: str, *, ready_when, timeout: float, interval: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last = {"ok": False, "error": "timed out"}
    while time.monotonic() < deadline:
        last = await bridge_call({"op": op, "request_id": request_id})
        if ready_when(last):
            return last
        await asyncio.sleep(interval)
    return last


async def register_pending(entries: list[dict[str, Any]], interaction: discord.Interaction, query: str, *, request_group_id: str = "") -> str:
    clean_entries = [entry for entry in (entries or []) if str((entry or {}).get("request_id") or "").strip()]
    if not clean_entries:
        return ""
    channel_id = interaction.channel_id or interaction.user.id
    batch_id = str(uuid.uuid4())
    request_ids = [str(entry["request_id"]).strip() for entry in clean_entries]
    batch = DownloadBatch(
        channel_id=channel_id,
        user_id=interaction.user.id,
        user_name=str(interaction.user),
        query=query,
        total_files=len(request_ids),
        request_ids=list(request_ids),
        items={
            request_id: DownloadItem(
                user=str(entry.get("user") or ""),
                path=str(entry.get("path") or ""),
                dest=str(entry.get("dest") or ""),
                label=download_file_label(str(entry.get("path") or "")),
            )
            for request_id, entry in [(str(item["request_id"]).strip(), item) for item in clean_entries]
        },
        latest="Queued",
    )
    state.batches[batch_id] = batch
    if request_group_id:
        state.download_groups[request_group_id] = batch_id
    for request_id in request_ids:
        state.pending[request_id] = PendingRequest(
            channel_id=channel_id,
            user_id=interaction.user.id,
            user_name=str(interaction.user),
            query=query,
            batch_id=batch_id,
        )
    state.save()
    return batch_id


async def register_reply(reply: dict[str, Any], interaction: discord.Interaction, query: str, *, request_group_id: str = "") -> str:
    entries = reply.get("entries") or [{"request_id": request_id} for request_id in (reply.get("request_ids") or [])]
    batch_id = await register_pending(entries, interaction, query, request_group_id=request_group_id)
    if batch_id:
        await ensure_batch_message(batch_id)
    return batch_id


def is_unshared_download_error(error: str) -> bool:
    text = str(error or "").strip().lower()
    return any(marker in text for marker in UNSHARED_ERROR_MARKERS)


def retryable_failed_items(batch: DownloadBatch) -> list[tuple[str, DownloadItem]]:
    return [
        (request_id, item)
        for request_id in batch.request_ids
        if (item := batch.items.get(request_id))
        and item.status == "error"
        and not is_unshared_download_error(item.error)
    ]


def unshared_failed_items(batch: DownloadBatch) -> list[tuple[str, DownloadItem]]:
    return [
        (request_id, item)
        for request_id in batch.request_ids
        if (item := batch.items.get(request_id))
        and item.status == "error"
        and is_unshared_download_error(item.error)
    ]


def remove_batch_group_mappings(batch_id: str) -> None:
    for group_id, mapped_batch_id in list(state.download_groups.items()):
        if mapped_batch_id == batch_id:
            state.download_groups.pop(group_id, None)


async def safe_edit(interaction: discord.Interaction, *, content: str, view=None):
    await interaction.edit_original_response(content=fit_discord_content(content), view=view)


async def safe_send(interaction: discord.Interaction, *, content: str, ephemeral: bool = True):
    await interaction.response.send_message(fit_discord_content(content), ephemeral=ephemeral)


def bandcamp_query_from_title(title: str) -> str:
    text = str(title or "").replace("“", '"').replace("”", '"').strip()
    if "," in text and '"' in text:
        artist, rest = text.split(",", 1)
        rest = rest.strip()
        if rest.startswith('"') and rest.endswith('"') and len(rest) > 2:
            album = rest.strip('"').strip()
            return f"{artist.strip()} {album}".strip()
    return text


def pitchfork_query_from_link(title: str, link: str) -> str:
    slug = ""
    try:
        parts = [part for part in urlsplit(str(link or "")).path.split("/") if part]
        if parts:
            slug = parts[-1].replace("-", " ").strip()
    except Exception:
        slug = ""
    return slug or str(title or "").strip()


async def ensure_upload_alert_message(alert_id: str, *, force: bool = False):
    alert = upload_alerts.get(alert_id)
    if alert is None:
        dirty_upload_alerts.discard(alert_id)
        return
    text = render_upload_alert(alert)
    view = None
    now = time.monotonic()
    if not force:
        last = upload_alert_last_edit_at.get(alert_id, 0.0)
        if alert.message_id and now - last < BATCH_EDIT_COOLDOWN_SECONDS:
            dirty_upload_alerts.add(alert_id)
            return
    message_id = await upsert_message(alert.channel_id, alert.message_id, text, view=view)
    if message_id:
        alert.message_id = message_id
        upload_alert_last_edit_at[alert_id] = now
        dirty_upload_alerts.discard(alert_id)


async def flush_dirty_upload_alerts():
    now = time.monotonic()
    for alert_id in list(dirty_upload_alerts):
        alert = upload_alerts.get(alert_id)
        if alert is None:
            dirty_upload_alerts.discard(alert_id)
            upload_alert_last_edit_at.pop(alert_id, None)
            continue
        if not alert.message_id or (now - upload_alert_last_edit_at.get(alert_id, 0.0)) >= BATCH_EDIT_COOLDOWN_SECONDS:
            await ensure_upload_alert_message(alert_id, force=True)


class BandcampAotdView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        button = discord.ui.Button(label="Download in Nicotine", style=discord.ButtonStyle.green, custom_id="bandcamp_aotd:download")

        async def callback(interaction: discord.Interaction):
            await interaction.response.defer(ephemeral=True, thinking=True)
            content = str(getattr(interaction.message, "content", "") or "")
            lines = [line.strip() for line in content.splitlines() if line.strip()]
            title = lines[1] if len(lines) > 1 else ""
            query = bandcamp_query_from_title(title)
            await start_album_search(interaction, query)

        button.callback = callback
        self.add_item(button)


class PitchforkBestNewAlbumView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        button = discord.ui.Button(label="Download in Nicotine", style=discord.ButtonStyle.green, custom_id="pitchfork_best_new_album:download")

        async def callback(interaction: discord.Interaction):
            await interaction.response.defer(ephemeral=True, thinking=True)
            content = str(getattr(interaction.message, "content", "") or "")
            lines = [line.strip() for line in content.splitlines() if line.strip()]
            title = lines[1] if len(lines) > 1 else ""
            link = lines[2] if len(lines) > 2 else ""
            query = pitchfork_query_from_link(title, link)
            await start_album_search(interaction, query)

        button.callback = callback
        self.add_item(button)


async def send_bandcamp_aotd_message(channel_id: int, title: str, link: str) -> None:
    channel = await get_channel(channel_id)
    if channel is None:
        return
    view = BandcampAotdView()
    message = (
        "Bandcamp Album of the Day\n"
        f"{title}\n"
        f"{link}"
    )
    try:
        await channel.send(fit_discord_content(message), view=view)
    except Exception:
        return


async def send_pitchfork_best_new_album_message(channel_id: int, title: str, link: str) -> None:
    channel = await get_channel(channel_id)
    if channel is None:
        return
    view = PitchforkBestNewAlbumView()
    message = (
        "Pitchfork Best New Album\n"
        f"{title}\n"
        f"{link}"
    )
    try:
        await channel.send(fit_discord_content(message), view=view)
    except Exception:
        return


async def record_upload_alert_event(event: dict[str, Any]):
    channel = await resolve_alert_channel()
    if channel is None:
        return
    user = str(event.get("user") or "unknown")
    path = str(event.get("path") or "")
    folder = str(event.get("folder") or path.rpartition("\\")[0] or "")
    alert_id = upload_alert_key(user, folder)
    label = str(event.get("folder_label") or download_album_label(folder or path) or "<root>")
    total_files = max(0, int(event.get("folder_total_files") or 0))
    file_label = download_file_label(path)
    alert = upload_alerts.get(alert_id)
    if alert is None:
        alert = UploadAlert(channel_id=channel.id, user=user, folder=folder, label=label, total_files=total_files)
        upload_alerts[alert_id] = alert
    else:
        alert.channel_id = channel.id
        if total_files > alert.total_files:
            alert.total_files = total_files
        if label and label != "<root>":
            alert.label = label
    kind = str(event.get("event") or "")
    if kind == "upload_started":
        alert.files[path] = "started"
    elif kind == "upload_finished":
        if path:
            alert.files[path] = "finished"
    else:
        return
    alert.latest = f"{'Started' if kind == 'upload_started' else 'Finished'} `{file_label}`"
    await ensure_upload_alert_message(alert_id)


async def get_channel(channel_id: int | None):
    if not channel_id:
        return None
    if channel := client.get_channel(channel_id):
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
    guild = guild or (client.guilds[0] if client.guilds else None)
    if guild is None:
        return None
    return guild.system_channel or (guild.text_channels[0] if guild.text_channels else None)


async def send_message_with_id(channel_id: int, text: str, view: discord.ui.View | None = None) -> int:
    channel = await get_channel(channel_id)
    if channel is None:
        return 0
    try:
        message = await channel.send(fit_discord_content(text), view=view)
        return int(message.id)
    except Exception:
        return 0


async def edit_message(channel_id: int, message_id: int, text: str, view: discord.ui.View | None = None) -> bool:
    if not message_id:
        return False
    channel = await get_channel(channel_id)
    if channel is None:
        return False
    try:
        message = await channel.fetch_message(int(message_id))
        await message.edit(content=fit_discord_content(text), view=view)
        return True
    except Exception:
        return False


async def upsert_message(channel_id: int, message_id: int, text: str, view: discord.ui.View | None = None) -> int:
    return message_id if message_id and await edit_message(channel_id, message_id, text, view=view) else await send_message_with_id(channel_id, text, view=view)


async def start_album_search(interaction: discord.Interaction, query: str) -> None:
    request_id = str(uuid.uuid4())
    reply = await bridge_call({"op": "search", "request_id": request_id, "query": query})
    if not reply.get("ok"):
        await safe_edit(interaction, content=f"Search failed: {reply}", view=None)
        return
    search = {"ok": False, "error": "timed out"}
    deadline = time.monotonic() + SEARCH_SLOW_RETRY_SECONDS
    slow_notice_sent = False
    while time.monotonic() < deadline:
        search = await bridge_call({"op": "search_results", "request_id": reply["request_id"]})
        if not search.get("ok"):
            break
        if search.get("results"):
            break
        elapsed = SEARCH_SLOW_RETRY_SECONDS - max(0.0, deadline - time.monotonic())
        if search.get("ready") and elapsed >= SEARCH_EMPTY_GRACE_SECONDS:
            await safe_edit(interaction, content=f"No results found for `{query}` after {int(SEARCH_EMPTY_GRACE_SECONDS)} seconds.", view=None)
            return
        if not slow_notice_sent and elapsed >= SEARCH_EMPTY_GRACE_SECONDS:
            await safe_edit(interaction, content=f"Search for `{query}` is still loading; I'll keep checking automatically for another minute.", view=None)
            slow_notice_sent = True
        await asyncio.sleep(SEARCH_POLL_INTERVAL_SECONDS)
    if not search.get("ok"):
        await safe_edit(interaction, content=f"Search failed: {search}", view=None)
        return
    if not (search.get("results") or []):
        if search.get("ready"):
            await safe_edit(interaction, content=f"No results found for `{query}` after {int(SEARCH_EMPTY_GRACE_SECONDS)} seconds.", view=None)
        else:
            await safe_edit(interaction, content=f"Search for `{query}` is still not returning data after automatic retries. Soulseek/Nicotine may be slow right now; try again later.", view=None)
        return
    full_search = await bridge_call({"op": "search_results", "request_id": reply["request_id"], "offset": 0, "limit": SEARCH_RESULTS_FETCH_LIMIT})
    results = full_search.get("results") or search.get("results") or []
    if not full_search.get("ok") or not results:
        await safe_edit(interaction, content=f"No results found for `{query}` after {int(SEARCH_EMPTY_GRACE_SECONDS)} seconds.", view=None)
        return
    session = {"query": query, "search_request_id": reply["request_id"], "results": results, "page": 0}
    await safe_edit(
        interaction,
        content=render_search_results_page(query, results, 0),
        view=SearchResultsView(session, results, page=0),
    )


class RetryFailedDownloadsView(discord.ui.View):
    def __init__(self, batch_id: str):
        super().__init__(timeout=None)
        self.batch_id = batch_id
        batch = state.batches.get(batch_id)
        has_retryable = bool(batch and retryable_failed_items(batch))
        has_unshared = bool(batch and unshared_failed_items(batch))
        if has_retryable:
            button = discord.ui.Button(
                label="Retry failed",
                style=discord.ButtonStyle.blurple,
                custom_id=f"retry_failed:{batch_id}",
            )
            button.callback = self.retry_failed
            self.add_item(button)
        if has_unshared:
            button = discord.ui.Button(
                label="Find another source",
                style=discord.ButtonStyle.green,
                custom_id=f"find_another_source:{batch_id}",
            )
            button.callback = self.find_another_source
            self.add_item(button)

    async def find_another_source(self, interaction: discord.Interaction):
        batch = state.batches.get(self.batch_id)
        if batch is None:
            await safe_send(interaction, content="That download batch is no longer active.", ephemeral=True)
            return
        query = str(batch.query or "").split(" :: ", 1)[0].strip()
        if not query:
            await safe_send(interaction, content="I don't have the original search text for that batch anymore.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        await start_album_search(interaction, query)

    async def retry_failed(self, interaction: discord.Interaction):
        batch = state.batches.get(self.batch_id)
        if batch is None:
            await safe_send(interaction, content="That download batch is no longer active.", ephemeral=True)
            return
        failed_items = retryable_failed_items(batch)
        if not failed_items:
            await safe_send(interaction, content="That failure is from a source that does not share the file anymore. Use Find another source instead.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        retried = 0
        failed = 0
        for request_id, item in failed_items:
            reply = await bridge_call({
                "op": "retry_download",
                "request_id": request_id,
                "user": item.user,
                "path": item.path,
                "dest": item.dest,
            })
            entries = reply.get("entries") or []
            if not reply.get("ok") or not entries:
                item.error = reply.get("error", "retry failed")
                failed += 1
                continue
            entry = entries[0]
            item.user = str(entry.get("user") or item.user)
            item.path = str(entry.get("path") or item.path)
            item.dest = str(entry.get("dest") or item.dest)
            item.label = download_file_label(item.path)
            item.progress = 0
            item.status = "retrying"
            item.error = ""
            state.pending[request_id] = PendingRequest(
                channel_id=batch.channel_id,
                user_id=batch.user_id,
                user_name=batch.user_name,
                query=batch.query,
                batch_id=self.batch_id,
            )
            retried += 1
        result_bits = [text for count, text in ((retried, f"retried {retried}"), (failed, f"failed {failed}")) if count]
        if result_bits:
            batch.latest = "Retry failed: " + ", ".join(result_bits)
        state.save()
        await ensure_batch_message(self.batch_id)
        result_text = ", ".join(result_bits) if result_bits else "nothing retried"
        await safe_edit(interaction, content=f"Retry result: {result_text}.", view=None)


async def ensure_batch_message(batch_id: str, *, force: bool = False):
    batch = state.batches.get(batch_id)
    if batch is None:
        dirty_batches.discard(batch_id)
        batch_last_edit_at.pop(batch_id, None)
        return False
    now = time.monotonic()
    if batch.message_id and not force and (now - batch_last_edit_at.get(batch_id, 0.0)) < BATCH_EDIT_COOLDOWN_SECONDS:
        dirty_batches.add(batch_id)
        return False
    text = render_batch_message(batch)
    view = RetryFailedDownloadsView(batch_id) if any(item.status == "error" for item in batch.items.values()) else None
    batch.message_id = await upsert_message(batch.channel_id, batch.message_id, text, view=view)
    state.save()
    if batch.message_id:
        batch_last_edit_at[batch_id] = time.monotonic()
        dirty_batches.discard(batch_id)
    return bool(batch.message_id)


async def flush_dirty_batch_updates():
    now = time.monotonic()
    for batch_id in list(dirty_batches):
        batch = state.batches.get(batch_id)
        if batch is None:
            dirty_batches.discard(batch_id)
            batch_last_edit_at.pop(batch_id, None)
            continue
        if not batch.message_id or (now - batch_last_edit_at.get(batch_id, 0.0)) >= BATCH_EDIT_COOLDOWN_SECONDS:
            await ensure_batch_message(batch_id, force=True)


async def remove_request_from_state(request_id: str, *, reason: str = "Removed from queue") -> None:
    pending = state.pending.pop(request_id, None)
    if pending is None:
        state.save()
        return
    batch_id = pending.batch_id
    batch = state.batches.get(batch_id)
    if batch is None:
        state.save()
        return
    item = batch.items.pop(request_id, None)
    batch.request_ids = [value for value in batch.request_ids if value != request_id]
    if batch.request_ids:
        batch.total_files = len(batch.request_ids)
        label = trim((item.label if item else "") or download_file_label(item.path if item else ""), 120) if item else request_id[:8]
        batch.latest = f"{reason}: `{label}`"
        state.save()
        await ensure_batch_message(batch_id, force=True)
        return
    channel_id = batch.channel_id
    message_id = batch.message_id
    state.batches.pop(batch_id, None)
    remove_batch_group_mappings(batch_id)
    state.save()
    dirty_batches.discard(batch_id)
    batch_last_edit_at.pop(batch_id, None)
    if message_id:
        await edit_message(channel_id, message_id, "Download queue cleared.", view=None)


async def remove_entries_from_state(entries: list[dict[str, Any]], *, reason: str) -> None:
    for entry in entries:
        if request_id := str(entry.get("request_id") or "").strip():
            await remove_request_from_state(request_id, reason=reason)


async def reconcile_state_with_bridge(*, force: bool = False) -> None:
    global last_reconcile_at
    now = time.monotonic()
    if not force and (now - last_reconcile_at) < RECONCILE_INTERVAL_SECONDS:
        return
    last_reconcile_at = now
    reply = await bridge_call({"op": "queue_list"})
    if not reply.get("ok"):
        return
    entries = reply.get("entries") or []
    live_request_ids = {
        str(entry.get("request_id") or "").strip()
        for entry in entries
        if str(entry.get("request_id") or "").strip()
        and str(entry.get("status") or "").strip().lower() not in TERMINAL_QUEUE_STATUSES
    }
    stale_request_ids = [request_id for request_id in list(state.pending) if request_id not in live_request_ids]
    touched_batches: set[str] = set()
    emptied_batches: list[tuple[int, int]] = []
    for request_id in stale_request_ids:
        pending = state.pending.pop(request_id, None)
        if pending is None:
            continue
        batch_id = pending.batch_id
        batch = state.batches.get(batch_id)
        if batch is None:
            continue
        batch.items.pop(request_id, None)
        batch.request_ids = [value for value in batch.request_ids if value != request_id]
        batch.total_files = len(batch.request_ids)
        touched_batches.add(batch_id)
        if not batch.request_ids:
            emptied_batches.append((batch.channel_id, batch.message_id))
            state.batches.pop(batch_id, None)
            remove_batch_group_mappings(batch_id)
            dirty_batches.discard(batch_id)
            batch_last_edit_at.pop(batch_id, None)
    healed_batches = heal_downloads_present_on_disk(state.batches)
    if stale_request_ids or healed_batches:
        state.save()
    for batch_id in touched_batches | healed_batches:
        if batch_id in state.batches:
            await ensure_batch_message(batch_id, force=True)
    for channel_id, message_id in emptied_batches:
        if message_id:
            await edit_message(channel_id, message_id, "Download queue cleared.", view=None)


class SearchResultSelect(discord.ui.Select):
    def __init__(self, results: list[dict[str, Any]], page: int):
        start = page * RESULT_PAGE_SIZE
        page_results = results[start:start + RESULT_PAGE_SIZE]
        super().__init__(
            placeholder="Pick a folder / album",
            min_values=1,
            max_values=1,
            options=[
                discord.SelectOption(label=result_label(result, start + index + 1), description=result_desc(result) or None, value=str(start + index))
                for index, result in enumerate(page_results)
            ],
        )
        self.results = results

    async def callback(self, interaction: discord.Interaction):
        await self.view.choose_result(interaction, self.results[int(self.values[0])])


async def attempt_browse_result(result: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    request_id = str(uuid.uuid4())
    reply = await bridge_call({"op": "browse_folder", "request_id": request_id, "user": result["user"], "folder": result.get("folder", "")})
    if not reply.get("ok"):
        return request_id, reply
    browse = await poll_bridge(
        "browse_folder_results",
        request_id,
        ready_when=lambda item: item.get("ok") and item.get("ready"),
        timeout=60.0,
        interval=2.0,
    )
    return request_id, browse


def render_browse_ready_text(result: dict[str, Any], files: list[dict[str, Any]]) -> str:
    return (
        f"Selected `{artist_album_text(result)}`\n"
        f"Matched audio files: {result_file_count(result)}\n"
        f"Folder tracks found: {len(files)}"
        + (f"\nDiscord will only show the first {TRACK_PICKER_LIMIT} tracks in the picker." if len(files) > TRACK_PICKER_LIMIT else "")
    )


async def retry_browse_result_later(interaction: discord.Interaction, session: dict[str, Any], result: dict[str, Any]) -> None:
    await asyncio.sleep(BROWSE_RETRY_DELAY_SECONDS)
    request_id, browse = await attempt_browse_result(result)
    if browse.get("ok") and browse.get("ready"):
        files = browse.get("files") or []
        session.update({"browse_request_id": request_id, "browse": browse, "folder": browse.get("folder", ""), "files": files})
        try:
            await safe_edit(interaction, content=render_browse_ready_text(result, files), view=BrowseDecisionView(session))
        except Exception:
            pass
        return
    try:
        await safe_edit(
            interaction,
            content="That library was still loading after the automatic retry. Use Download all now, or try Pick specific tracks again later.",
            view=ResultDecisionView(session, result),
        )
    except Exception:
        pass


class SearchResultsView(discord.ui.View):
    def __init__(self, session: dict[str, Any], results: list[dict[str, Any]], page: int = 0):
        super().__init__(timeout=600)
        self.session = session
        self.results = results
        self.page = max(0, page)
        self._rebuild_items()

    def _rebuild_items(self) -> None:
        self.clear_items()
        if self.results:
            self.add_item(SearchResultSelect(self.results, self.page))
        max_page = max(0, (len(self.results) - 1) // RESULT_PAGE_SIZE) if self.results else 0
        for label, delta, disabled in (("Prev 10", -1, self.page <= 0), ("Next 10", 1, self.page >= max_page)):
            button = discord.ui.Button(label=label, style=discord.ButtonStyle.secondary, disabled=disabled)

            async def callback(interaction: discord.Interaction, page_delta=delta):
                await self.change_page(interaction, self.page + page_delta)

            button.callback = callback
            self.add_item(button)

    async def change_page(self, interaction: discord.Interaction, new_page: int):
        max_page = max(0, (len(self.results) - 1) // RESULT_PAGE_SIZE) if self.results else 0
        self.page = max(0, min(new_page, max_page))
        self.session["page"] = self.page
        self._rebuild_items()
        await interaction.response.edit_message(content=render_search_results_page(self.session.get("query", "album"), self.results, self.page), view=self)

    async def choose_result(self, interaction: discord.Interaction, result: dict[str, Any]):
        self.session["selected_result"] = result
        search_request_id = str(self.session.get("search_request_id") or "").strip()
        if search_request_id:
            try:
                await bridge_call({"op": "clear_search", "request_id": search_request_id})
            except Exception:
                pass
        text = (
            f"Selected `{artist_album_text(result)}`\n"
            f"Matched audio files in search: {result_file_count(result)}\n"
            "Use Download all for the fast folder download. Pick specific tracks only if you want to browse their library."
        )
        await interaction.response.edit_message(content=text, view=ResultDecisionView(self.session, result))


class ResultDecisionView(discord.ui.View):
    def __init__(self, session: dict[str, Any], result: dict[str, Any]):
        super().__init__(timeout=600)
        self.session = session
        self.result = result

    @discord.ui.button(label="Download all", style=discord.ButtonStyle.green)
    async def download_all(self, interaction: discord.Interaction, _button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        request_group_id = str(uuid.uuid4())
        reply = await bridge_call({
            "op": "download_search_folder",
            "request_group_id": request_group_id,
            "user": self.result.get("user", ""),
            "folder": self.result.get("folder", ""),
            "visible_files": self.result.get("visible_files") or [],
            "dest": "",
        })
        if not reply.get("ok"):
            await safe_edit(interaction, content=f"Download failed: {reply}", view=None)
            return
        await register_reply(
            reply,
            interaction,
            f"{self.session.get('query', 'album')} :: {self.result.get('folder', '<root>')}",
            request_group_id=str(reply.get("request_group_id") or request_group_id),
        )
        queued = int(reply.get("queued", 0) or 0)
        extra = " and requested the rest of the folder" if reply.get("waiting_for_folder_contents") else ""
        await safe_edit(interaction, content=f"Queued {queued} visible file(s){extra}. Progress will update in-channel.", view=None)

    @discord.ui.button(label="Pick specific tracks", style=discord.ButtonStyle.blurple)
    async def pick_specific(self, interaction: discord.Interaction, _button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        request_id, browse = await attempt_browse_result(self.result)
        if not browse.get("ok") or not browse.get("ready"):
            asyncio.create_task(retry_browse_result_later(interaction, dict(self.session), dict(self.result)))
            await safe_edit(
                interaction,
                content="That library is still loading / parsing on their side. I'll retry the track picker automatically in 5 minutes and update this message.",
                view=ResultDecisionView(self.session, self.result),
            )
            return
        files = browse.get("files") or []
        self.session.update({"browse_request_id": request_id, "browse": browse, "folder": browse.get("folder", ""), "files": files})
        await safe_edit(interaction, content=render_browse_ready_text(self.result, files), view=BrowseDecisionView(self.session))


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
        await register_reply(reply, interaction, f"{self.session.get('query', 'album')} :: {self.session.get('folder', '<root>')}")
        await safe_edit(interaction, content=f"Queued {reply.get('queued', 0)} file(s) for download. Progress will update in-channel.", view=None)

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
                discord.SelectOption(label=trim(item.get("name") or item.get("fullpath") or "file", 100), description=trim(item.get("size_human") or "", 100) or None, value=str(index))
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
        await register_reply(reply, interaction, f"{self.session.get('query', 'album')} :: {self.session.get('folder', '<root>')}")
        await safe_edit(interaction, content=f"Queued {reply.get('queued', 0)} selected track(s). Progress will update in-channel.", view=None)


async def sync_slash_commands():
    try:
        if guild_obj is not None:
            tree.copy_global_to(guild=guild_obj)
        synced = await tree.sync(guild=guild_obj) if guild_obj is not None else await tree.sync()
        print(f"slash sync ok ({'guild' if guild_obj is not None else 'global'}): {len(synced)} commands")
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
    global announcement_views_registered
    if not announcement_views_registered:
        client.add_view(BandcampAotdView())
        client.add_view(PitchforkBestNewAlbumView())
        announcement_views_registered = True
    if not watch_bridge_events.is_running():
        watch_bridge_events.start()
    await reconcile_state_with_bridge(force=True)
    for batch_id, batch in list(state.batches.items()):
        if batch.message_id:
            client.add_view(RetryFailedDownloadsView(batch_id))
    print(f"Logged in as {client.user} | socket={BRIDGE_SOCKET} | events={EVENTS_FILE} | state={STATE_FILE}")


slsk = app_commands.Group(name="slsk", description="Soulseek / Nicotine bridge commands")


async def bridge_status_text() -> str:
    started = time.monotonic()
    reply = await bridge_call({"op": "status"})
    latency_ms = max(0, round((time.monotonic() - started) * 1000))
    if not reply.get("ok"):
        if "unknown op" in str(reply.get("error") or "").lower():
            legacy_ping = await bridge_call({"op": "ping"})
            if legacy_ping.get("ok"):
                return (
                    f"Bridge online ({latency_ms} ms)\n"
                    "Detailed health is waiting for the Nicotine plugin to be reloaded."
                )
        return f"Bridge offline: {reply.get('error') or 'unknown error'}"
    connected = reply.get("soulseek_connected")
    connection_text = "connected" if connected is True else "disconnected" if connected is False else "unknown"
    pending = int(reply.get("pending_requests") or 0)
    active = int(reply.get("active_requests") or 0)
    transfers = int(reply.get("download_transfers") or 0)
    uptime = int(reply.get("uptime_seconds") or 0)
    return (
        f"Bridge online ({latency_ms} ms)\n"
        f"Soulseek: {connection_text}\n"
        f"Tracked requests: {active} active, {pending} pending\n"
        f"Nicotine download rows: {transfers}\n"
        f"Plugin uptime: {uptime // 3600}h {(uptime % 3600) // 60}m"
    )


@slsk.command(name="ping", description="Check that the local Nicotine bridge is alive")
async def slsk_ping(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)
    await safe_edit(interaction, content=await bridge_status_text())


@slsk.command(name="status", description="Show Nicotine, Soulseek, bridge, and queue health")
async def slsk_status(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)
    await safe_edit(interaction, content=await bridge_status_text())


@slsk.command(name="album", description="Search Soulseek for an album or folder, then choose what to download")
@app_commands.describe(query="Album / artist / folder search text")
async def slsk_album(interaction: discord.Interaction, query: str):
    await interaction.response.defer(ephemeral=True, thinking=True)
    await start_album_search(interaction, query)


@slsk.command(name="download", description="Queue an exact Soulseek username + path download")
@app_commands.describe(user="Soulseek username", path="Virtual path inside the user's share", destination="Optional local destination folder")
async def slsk_download(interaction: discord.Interaction, user: str, path: str, destination: str | None = None):
    await interaction.response.defer(ephemeral=True, thinking=True)
    reply = await bridge_call({
        "op": "download",
        "request_id": str(uuid.uuid4()),
        "user": user,
        "path": path,
        "dest": destination or "",
    })
    if not reply.get("ok"):
        await safe_edit(interaction, content=f"Failed: {reply}", view=None)
        return
    await register_reply(reply, interaction, f"{user} :: {path}")
    await safe_edit(interaction, content=f"Queued exact path download for `{user}` -> `{path}`. Progress will update in-channel.", view=None)


@slsk.command(name="queue", description="Show the current Nicotine bridge download queue")
@app_commands.describe(limit="How many queued items to show (default 15, max 25)")
async def slsk_queue(interaction: discord.Interaction, limit: app_commands.Range[int, 1, 25] = 15):
    await interaction.response.defer(ephemeral=True, thinking=True)
    reply = await bridge_call({"op": "queue_list"})
    if not reply.get("ok"):
        await safe_edit(interaction, content=f"Queue lookup failed: {reply}", view=None)
        return
    await safe_edit(interaction, content=render_queue_entries(reply.get("entries") or [], limit=limit), view=None)


@slsk.command(name="unqueue", description="Remove queued downloads by request id/prefix or by bulk filters")
@app_commands.describe(
    request="Optional request id or unique prefix from /slsk queue",
    user="Optional Soulseek username to bulk filter by",
    path_contains="Optional text that must appear in the queued path",
)
async def slsk_unqueue(
    interaction: discord.Interaction,
    request: str | None = None,
    user: str | None = None,
    path_contains: str | None = None,
):
    await interaction.response.defer(ephemeral=True, thinking=True)
    if request:
        reply = await bridge_call({"op": "queue_remove", "request_id": request})
        if not reply.get("ok"):
            await safe_edit(interaction, content=f"Queue removal failed: {reply}", view=None)
            return
        removed = reply.get("removed") or []
        await remove_entries_from_state(removed, reason="Removed from queue")
        if removed:
            entry = removed[0]
            label = trim(download_file_label(str(entry.get("path") or "")), 120)
            owner = trim(str(entry.get("user") or "unknown"), 80)
            await safe_edit(interaction, content=f"Removed `{label}` from `{owner}` queue.", view=None)
            return
        await safe_edit(interaction, content=str(reply.get("message") or "Queue item removed."), view=None)
        return
    if not (user or path_contains):
        await safe_edit(interaction, content="Give me either request, user, or path_contains.", view=None)
        return
    reply = await bridge_call({
        "op": "queue_remove_matching",
        "user": user or "",
        "path_contains": path_contains or "",
    })
    if not reply.get("ok"):
        await safe_edit(interaction, content=f"Bulk queue removal failed: {reply}", view=None)
        return
    removed = reply.get("removed") or []
    await remove_entries_from_state(removed, reason="Bulk removed from queue")
    filters = []
    if user:
        filters.append(f"user `{trim(user, 80)}`")
    if path_contains:
        filters.append(f"path containing `{trim(path_contains, 80)}`")
    target = " and ".join(filters) if filters else "filters"
    await safe_edit(
        interaction,
        content=f"Removed {len(removed)} queued item(s) matching {target}.",
        view=None,
    )


tree.add_command(slsk)


def load_local_extensions() -> None:
    extension_dir = Path(__file__).resolve().parent / "local_extensions"
    if not extension_dir.is_dir():
        return
    deps = {
        "tree": tree,
        "discord": discord,
        "app_commands": app_commands,
        "safe_edit": safe_edit,
        "safe_send": safe_send,
        "trim": trim,
        "fit_discord_content": fit_discord_content,
    }
    for path in sorted(extension_dir.glob("*.py")):
        if path.name.startswith("_"):
            continue
        module_name = f"local_extension_{path.stem}"
        try:
            spec = importlib.util.spec_from_file_location(module_name, path)
            if spec is None or spec.loader is None:
                print(f"local extension skipped {path.name}: no loader")
                continue
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            setup = getattr(module, "setup", None)
            if callable(setup):
                setup(deps)
                print(f"local extension loaded: {path.name}")
        except Exception as exc:
            print(f"local extension failed {path.name}: {exc}")


load_local_extensions()


@tasks.loop(seconds=2.5)
async def watch_bridge_events():
    if not EVENTS_FILE.exists():
        await flush_dirty_batch_updates()
        await flush_dirty_upload_alerts()
        await reconcile_state_with_bridge()
        return
    try:
        lines, state.cursor, cursor_reset = read_event_lines(EVENTS_FILE, state.cursor)
        if cursor_reset:
            print(f"event log rotation detected; cursor reset for {EVENTS_FILE}")
    except Exception as exc:
        print(f"event log read failed: {exc}")
        await flush_dirty_batch_updates()
        await flush_dirty_upload_alerts()
        await reconcile_state_with_bridge()
        return
    touched_batches: dict[str, bool] = {}
    for raw in lines:
        try:
            event = json.loads(raw.strip())
        except Exception:
            continue
        kind = event.get("event")
        request_id = event.get("request_id")
        if kind == "bandcamp_aotd":
            channel_id = int(event.get("channel_id") or ALERT_CHANNEL_ID or 0)
            title = str(event.get("title") or "Bandcamp Album of the Day")
            link = str(event.get("link") or "https://daily.bandcamp.com/album-of-the-day")
            if channel_id:
                await send_bandcamp_aotd_message(channel_id, title, link)
            continue
        if kind == "pitchfork_best_new_album":
            channel_id = int(event.get("channel_id") or ALERT_CHANNEL_ID or 0)
            title = str(event.get("title") or "Pitchfork Best New Album")
            link = str(event.get("link") or "https://pitchfork.com/feed/reviews/best/albums/rss")
            if channel_id:
                await send_pitchfork_best_new_album_message(channel_id, title, link)
            continue
        if kind in {"upload_started", "upload_finished"}:
            await record_upload_alert_event(event)
            continue
        if kind == "removed" and request_id:
            await remove_request_from_state(request_id, reason=str(event.get("reason") or "Removed from queue"))
            continue
        if kind == "queued" and request_id:
            request_group_id = str(event.get("request_group_id") or "").strip()
            batch_id = state.download_groups.get(request_group_id, "") if request_group_id else ""
            if batch_id and request_id not in state.pending and (batch := state.batches.get(batch_id)):
                path = str(event.get("path") or "")
                batch.request_ids.append(request_id)
                batch.total_files = len(batch.request_ids)
                batch.items[request_id] = DownloadItem(
                    user=str(event.get("user") or ""),
                    path=path,
                    dest=str(event.get("dest") or ""),
                    label=download_file_label(path),
                )
                batch.latest = f"Queued `{download_file_label(path)}`"
                state.pending[request_id] = PendingRequest(
                    channel_id=batch.channel_id,
                    user_id=batch.user_id,
                    user_name=batch.user_name,
                    query=batch.query,
                    batch_id=batch_id,
                )
                touched_batches[batch_id] = True
            continue
        if not request_id or request_id not in state.pending:
            continue
        pending = state.pending[request_id]
        batch_id = pending.batch_id
        if not (batch := state.batches.get(batch_id)) or not (item := batch.items.get(request_id)):
            continue
        path_label = item.label or download_file_label(str(event.get("path") or item.path or ""))
        force_refresh = False
        if kind == "started":
            item.status = "started"
            item.progress = max(item.progress, 0)
            item.error = ""
            batch.latest = f"Started `{path_label}`"
        elif kind == "progress":
            percent = max(0, min(99, int(event.get("percent") or 0)))
            item.status = "progress"
            item.progress = max(percent, item.progress)
            batch.latest = f"{path_label} reached {percent}%"
        elif kind == "finished":
            item.status = "finished"
            item.progress = 100
            item.error = ""
            batch.latest = f"Finished `{path_label}`"
            state.pending.pop(request_id, None)
            force_refresh = batch_completed_count(batch) >= batch.total_files
        elif kind == "error":
            item.status = "error"
            item.error = str(event.get('error', 'unknown error'))
            batch.latest = f"Error on `{path_label}`: {item.error}"
            state.pending.pop(request_id, None)
        else:
            continue
        touched_batches[batch_id] = touched_batches.get(batch_id, False) or force_refresh
    if lines:
        state.save()
    completed_batches: list[str] = []
    for batch_id, force_refresh in touched_batches.items():
        await ensure_batch_message(batch_id, force=force_refresh)
        batch = state.batches.get(batch_id)
        if batch is not None and batch.request_ids and batch_completed_count(batch) >= batch.total_files:
            completed_batches.append(batch_id)
    if completed_batches:
        for batch_id in completed_batches:
            state.batches.pop(batch_id, None)
            remove_batch_group_mappings(batch_id)
            dirty_batches.discard(batch_id)
            batch_last_edit_at.pop(batch_id, None)
        state.save()
    await flush_dirty_batch_updates()
    await flush_dirty_upload_alerts()
    await reconcile_state_with_bridge()


if __name__ == "__main__":
    if not DISCORD_TOKEN:
        raise SystemExit(f"Set DISCORD_TOKEN in {ENV_PATH} or the environment first.")
    client.run(DISCORD_TOKEN)
