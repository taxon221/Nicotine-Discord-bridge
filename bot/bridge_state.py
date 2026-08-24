from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


def read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def read_event_lines(path: Path, cursor: int) -> tuple[list[str], int, bool]:
    """Read complete JSONL records and recover if the file was rotated."""
    with path.open("rb") as handle:
        size = os.fstat(handle.fileno()).st_size
        reset = cursor < 0 or cursor > size
        start = 0 if reset else cursor
        handle.seek(start)
        data = handle.read()
    complete_end = data.rfind(b"\n") + 1
    if complete_end <= 0:
        return [], start, reset
    complete = data[:complete_end]
    return [raw.decode("utf-8", errors="replace") for raw in complete.splitlines(keepends=True)], start + complete_end, reset


@dataclass
class PendingRequest:
    channel_id: int
    user_id: int
    user_name: str
    query: str
    batch_id: str = ""


@dataclass
class DownloadItem:
    user: str
    path: str
    dest: str = ""
    label: str = ""
    progress: int = 0
    status: str = "queued"
    error: str = ""


@dataclass
class DownloadBatch:
    channel_id: int
    user_id: int
    user_name: str
    query: str
    total_files: int
    request_ids: list[str] = field(default_factory=list)
    items: dict[str, DownloadItem] = field(default_factory=dict)
    message_id: int = 0
    latest: str = ""


def heal_downloads_present_on_disk(batches: dict[str, DownloadBatch]) -> set[str]:
    """Mark errored downloads complete when their destination file is present."""
    healed: set[str] = set()
    for batch_id, batch in batches.items():
        changed = False
        for item in batch.items.values():
            if str(item.status or "").lower() != "error" or not item.dest or not item.label:
                continue
            if (Path(item.dest).expanduser() / item.label).exists():
                item.status = "finished"
                item.progress = 100
                item.error = ""
                changed = True
        if not changed:
            continue
        healed.add(batch_id)
        if batch.request_ids and all(
            (item := batch.items.get(request_id)) is not None and str(item.status or "").lower() == "finished"
            for request_id in batch.request_ids
        ):
            batch.latest = f"Finished {len(batch.request_ids)}/{len(batch.request_ids)}"
    return healed


@dataclass
class UploadAlert:
    channel_id: int
    user: str
    folder: str
    label: str
    total_files: int = 0
    files: dict[str, str] = field(default_factory=dict)
    message_id: int = 0
    latest: str = ""


class BridgeState:
    def __init__(self, path: Path):
        self.path = path
        self.cursor = 0
        self.pending: dict[str, PendingRequest] = {}
        self.batches: dict[str, DownloadBatch] = {}
        self.download_groups: dict[str, str] = {}
        self.load()

    def load(self):
        data = read_json(self.path)
        self.cursor = int(data.get("cursor", 0) or 0)
        self.download_groups = {str(key): str(value) for key, value in (data.get("download_groups") or {}).items() if str(key).strip() and str(value).strip()}
        self.pending = {
            str(key): PendingRequest(**value)
            for key, value in (data.get("pending") or {}).items()
        }
        self.batches = {
            str(key): DownloadBatch(
                **{
                    **value,
                    "items": {str(item_key): DownloadItem(**item_value) for item_key, item_value in (value.get("items") or {}).items()},
                }
            )
            for key, value in (data.get("batches") or {}).items()
        }

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "cursor": self.cursor,
            "pending": {key: asdict(value) for key, value in self.pending.items()},
            "download_groups": dict(self.download_groups),
            "batches": {key: asdict(value) for key, value in self.batches.items()},
        }
        temp_path = self.path.with_name(self.path.name + ".tmp")
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, self.path)
