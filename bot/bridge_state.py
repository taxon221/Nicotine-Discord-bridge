from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


def read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


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
        self.path.write_text(
            json.dumps(
                {
                    "cursor": self.cursor,
                    "pending": {key: asdict(value) for key, value in self.pending.items()},
                    "download_groups": dict(self.download_groups),
                    "batches": {key: asdict(value) for key, value in self.batches.items()},
                },
                indent=2,
            ),
            encoding="utf-8",
        )
