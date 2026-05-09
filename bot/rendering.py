from __future__ import annotations

from typing import Any

from bridge_state import DownloadBatch, DownloadItem, UploadAlert


DISCORD_CONTENT_LIMIT = 2000
RESULT_PAGE_SIZE = 10


def trim(text: str, limit: int) -> str:
    text = str(text or "")
    return text if len(text) <= limit else text[: max(0, limit - 1)] + "…"


def fit_discord_content(text: str) -> str:
    return trim(text, DISCORD_CONTENT_LIMIT)


def download_file_label(path: str) -> str:
    parts = [part for part in str(path or "").split("\\") if part]
    return trim(parts[-1] if parts else (path or "file"), 120)


def download_album_label(path: str) -> str:
    parts = [part for part in str(path or "").split("\\") if part]
    if len(parts) >= 2:
        album = parts[-2]
        if len(parts) >= 3:
            artist = parts[-3]
            if artist and artist.lower() != album.lower():
                return trim(f"{artist} — {album}", 120)
        return trim(album, 120)
    return download_file_label(path)


def item_progress(item: DownloadItem) -> int:
    return max(0, min(100, int(item.progress or 0)))


def batch_percent(batch: DownloadBatch) -> int:
    values = [item_progress(batch.items.get(request_id, DownloadItem(user="", path=""))) for request_id in batch.request_ids]
    return max(0, min(100, int(round(sum(values) / len(values))))) if values else 0


def batch_completed_count(batch: DownloadBatch) -> int:
    return sum(1 for request_id in batch.request_ids if batch.items.get(request_id) and batch.items[request_id].status == "finished")


def progress_markers(percent: int) -> str:
    return " ".join(f"{'●' if percent >= threshold else '○'}{threshold}" for threshold in (0, 25, 50, 75, 100))


def active_batch_item(batch: DownloadBatch) -> tuple[int, DownloadItem | None]:
    for index, request_id in enumerate(batch.request_ids, start=1):
        item = batch.items.get(request_id)
        if item is not None and item.status in {"started", "progress", "retrying", "queued", "error"}:
            return index, item
    if batch.request_ids:
        return len(batch.request_ids), batch.items.get(batch.request_ids[-1])
    return 0, None


def current_item_heading(item: DownloadItem) -> str:
    return {
        "finished": "Done",
        "error": "Failed",
        "started": "Downloading",
        "retrying": "Retrying",
        "queued": "Queued",
        "progress": "Downloading",
    }.get(item.status, "Current")


def final_batch_message(batch: DownloadBatch, *, failed: int) -> str:
    first_item = next((batch.items.get(request_id) for request_id in batch.request_ids if batch.items.get(request_id) is not None), None)
    source_user = trim(first_item.user, 80) if first_item and first_item.user else "someone"
    if batch.total_files <= 1 and first_item is not None:
        label = trim(first_item.label or download_file_label(first_item.path), 120)
        return fit_discord_content(f"Download `{label}` from `{source_user}` finished!")
    target = trim(download_album_label(first_item.path), 120) if first_item and first_item.path else trim(batch.query, 120)
    summary = f"Download `{target}` from `{source_user}` finished!"
    if batch.total_files > 1:
        summary += f" ({batch.total_files} files)"
    if failed:
        summary += f" • {failed} failed"
    return fit_discord_content(summary)


def render_batch_message(batch: DownloadBatch) -> str:
    percent = batch_percent(batch)
    done = batch_completed_count(batch)
    failed = sum(1 for request_id in batch.request_ids if batch.items.get(request_id) and batch.items[request_id].status == "error")
    if done >= batch.total_files and failed == 0:
        return final_batch_message(batch, failed=failed)
    title = "Download complete" if percent >= 100 and done >= batch.total_files else "Download progress"
    lines = [
        f"{title}: `{trim(batch.query, 140)}`",
        f"Overall: {percent}%",
        f"Files: {done}/{batch.total_files} finished" + (f" • {failed} failed" if failed else ""),
    ]
    current_index, current_item = active_batch_item(batch)
    if current_item is not None:
        label = trim(current_item.label or download_file_label(current_item.path), 100)
        lines.append(f"{current_item_heading(current_item)}: {current_index}/{batch.total_files} `{label}`")
        lines.append(f"Track: [{progress_markers(item_progress(current_item))}]")
        if current_item.status == "error" and current_item.error:
            lines.append(f"Error: {trim(current_item.error, 140)}")
    if batch.latest and (current_item is None or current_item.status != "error"):
        lines.append(f"Latest: {trim(batch.latest, 180)}")
    if failed:
        lines.append("Use Retry failed on the message to requeue failed tracks.")
    return fit_discord_content("\n".join(lines))


def artist_album_text(result: dict[str, Any]) -> str:
    artist = str(result.get("artist") or "").strip()
    album = str(result.get("album") or (str(result.get("folder") or "").split("\\")[-1] or "<root>")).strip() or "<root>"
    return f"{artist} — {album}" if artist and artist.lower() != album.lower() else album


def result_file_count(result: dict[str, Any]) -> int:
    return int(result.get("display_file_count") or result.get("audio_file_count") or result.get("match_count") or 0)


def result_label(result: dict[str, Any], index: int | None = None) -> str:
    prefix = f"{index}. " if index is not None else ""
    return trim(f"{prefix}{artist_album_text(result)}", 100)


def result_desc(result: dict[str, Any]) -> str:
    format_summary = str(result.get("format_summary") or "").strip()
    return trim(" • ".join(part for part in (f"{result_file_count(result)} files", format_summary) if part), 100)


def render_search_results_page(query: str, results: list[dict[str, Any]], page: int) -> str:
    total = len(results)
    if total <= 0:
        return f"No useful results found for `{query}` yet."
    max_page = max(0, (total - 1) // RESULT_PAGE_SIZE)
    page = max(0, min(page, max_page))
    start = page * RESULT_PAGE_SIZE
    end = min(total, start + RESULT_PAGE_SIZE)
    lines = [f"Results {start + 1}-{end} of {total} for `{query}`:"]
    for index, result in enumerate(results[start:end], start=start + 1):
        lines.append(f"{index}. {result_label(result)} ({result_file_count(result)} files, {result.get('format_summary', 'unknown audio')})")
    if total > RESULT_PAGE_SIZE:
        lines.append("Use Prev 10 / Next 10 to page through results.")
    lines.append("Use the dropdown to choose the folder / album you want.")
    return "\n".join(lines)


def upload_alert_key(user: str, folder: str) -> str:
    return f"{str(user or '')}\0{str(folder or '')}"


def render_upload_alert(alert: UploadAlert) -> str:
    return fit_discord_content(f"Someone started downloading from you: `{trim(alert.user or 'unknown', 80)}`\nFolder: `{trim(alert.label or '<root>', 140)}`\nFiles downloaded: {sum(1 for status in alert.files.values() if status == 'finished')}")


def render_queue_entries(entries: list[dict[str, Any]], *, limit: int = 15) -> str:
    shown = list(entries[: max(1, limit)])
    if not shown:
        return "Bridge queue is empty."
    lines = [f"Bridge queue ({len(entries)} item(s)):"]
    for index, entry in enumerate(shown, start=1):
        request_id = str(entry.get("request_id") or "")
        short_id = request_id[:8] or "unknown"
        user = trim(str(entry.get("user") or "unknown"), 40)
        label = trim(download_file_label(str(entry.get("path") or "")), 90)
        status = str(entry.get("status") or "Queued")
        percent = max(0, min(100, int(entry.get("percent") or 0)))
        active = bool(entry.get("active"))
        queue_index = int(entry.get("queue_index") or 0)
        queue_depth = int(entry.get("queue_depth") or 0)
        queue_position = int(entry.get("queue_position") or 0)
        detail = status
        if active and percent:
            detail += f" {percent}%"
        elif queue_position:
            detail += f" pos {queue_position}"
        elif queue_depth > 1:
            detail += f" dup {queue_index}/{queue_depth}"
        prefix = "▶" if active else "•"
        lines.append(f"{index}. {prefix} [{short_id}] {user} — `{label}` ({detail})")
    if len(entries) > len(shown):
        lines.append(f"…and {len(entries) - len(shown)} more.")
    lines.append("Remove one with /slsk unqueue request:<id or prefix>")
    lines.append("Or bulk remove with /slsk unqueue [user:name] [path_contains:text]")
    return fit_discord_content("\n".join(lines))
