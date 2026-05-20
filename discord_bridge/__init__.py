from __future__ import annotations

import json
import os
import re
import socket
import subprocess
import threading
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

from gi.repository import GLib
from pynicotine.pluginsystem import BasePlugin


AUDIO_EXTENSIONS = {
    "aac", "aif", "aiff", "alac", "ape", "dsf", "flac", "m4a", "m4b", "mid", "midi",
    "mp2", "mp3", "mpc", "ogg", "oga", "opus", "ra", "tak", "tta", "wav", "wma", "wv",
}
GENERIC_FOLDER_NAMES = {
    "albums", "album", "audio", "complete", "completed", "discography", "downloads", "flac",
    "library", "lossless", "lossy", "media", "mixes", "mp3", "music", "new", "release", "releases",
    "rips", "shared", "share", "shares", "shared files", "soulseek", "unsorted", "various artists", "va", "web",
}
SETTING_SPECS = {
    "results_limit": (5, "Set how many album or folder results Discord shows per search", "integer"),
    "track_picker_limit": (25, "Set how many tracks Discord shows in the picker (max 25)", "integer"),
    "bot_env_path": (str(Path.home() / "nicotine-discord-bridge" / ".env"), "Set the Discord bot .env file path used by /bridgeenv", "string"),
    "emit_upload_started": (True, "Write upload started events for Discord alerts", "bool"),
    "emit_upload_finished": (True, "Write upload finished events for Discord alerts", "bool"),
    "emit_download_started": (True, "Write download started events for Discord alerts", "bool"),
    "emit_download_finished": (True, "Write download finished events for Discord alerts", "bool"),
    "upload_alert_mode": ("file", "Set Discord alert granularity for uploads: file or album", "string"),
    "download_alert_mode": ("file", "Set Discord alert granularity for downloads: file or album", "string"),
}
DEFAULT_SETTINGS = {key: default for key, (default, _description, _value_type) in SETTING_SPECS.items()}
METASETTINGS = {key: {"description": description, "type": value_type} for key, (_default, description, value_type) in SETTING_SPECS.items()}


class Plugin(BasePlugin):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        base_dir = Path.home() / ".local" / "share" / "nicotine" / "discord-bridge"
        self.base_dir = base_dir
        self.socket_path = base_dir / "control.sock"
        self.events_path = base_dir / "events.jsonl"
        self.state_path = base_dir / "state.json"
        self.runtime_path = base_dir / "runtime.json"
        self.settings = dict(DEFAULT_SETTINGS)
        self.metasettings = {key: dict(value) for key, value in METASETTINGS.items()}
        self.__privatecommands__ = [("bridgeenv", self.open_env_command), ("bridgepaths", self.show_paths_command)]
        self._stop = threading.Event()
        self._server_thread = self._progress_thread = self._server_socket = None
        self._lock = threading.RLock()
        self._pending = defaultdict(deque)
        self._active = {}
        self._progress_marks = {}
        self._search_sessions = {}
        self._browse_sessions = {}
        self._folder_download_sessions = {}

    def init(self):
        self._ensure_dirs()

    def loaded_notification(self):
        self._ensure_dirs()
        self._load_state()
        self._prune_stale_state()
        self._write_runtime_manifest()
        self._start_server()
        self._start_progress_watcher()
        self.log(f"Discord bridge ready at {self.socket_path} (events={self.events_path})")

    def disable(self):
        self._shutdown()

    shutdown_notification = disable
    unloaded_notification = disable

    def _ensure_dirs(self):
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _safe_int(self, value, default):
        try:
            return int(value)
        except Exception:
            return default

    def _setting_limit(self, key: str, default: int) -> int:
        return max(1, min(25, self._safe_int(self.settings.get(key), default)))

    def _bot_env_path(self) -> Path:
        return Path(str(self.settings.get("bot_env_path") or Path.home() / "nicotine-discord-bridge" / ".env")).expanduser()

    def _alert_mode(self, key: str, default: str = "file") -> str:
        value = str(self.settings.get(key) or default).strip().lower()
        return value if value in {"file", "album"} else default

    def _runtime_manifest(self) -> dict:
        return {
            "data_dir": str(self.base_dir),
            "socket_path": str(self.socket_path),
            "events_path": str(self.events_path),
            "state_path": str(self.state_path),
            "results_limit": self._setting_limit("results_limit", 5),
            "track_picker_limit": self._setting_limit("track_picker_limit", 25),
            "bot_env_path": str(self._bot_env_path()),
            **{key: bool(self.settings.get(key, True)) for key in ("emit_upload_started", "emit_upload_finished", "emit_download_started", "emit_download_finished")},
            "upload_alert_mode": self._alert_mode("upload_alert_mode"),
            "download_alert_mode": self._alert_mode("download_alert_mode"),
        }

    def _write_runtime_manifest(self):
        try:
            self.runtime_path.write_text(json.dumps(self._runtime_manifest(), indent=2, ensure_ascii=False), encoding="utf-8")
        except Exception as exc:
            self.log(f"Discord bridge runtime manifest write failed: {exc}")

    def open_env_command(self, _source, _args):
        env_path = self._bot_env_path()
        try:
            subprocess.Popen(["xdg-open", str(env_path)])
            self.echo_message(f"Opened Discord bot env file: {env_path}")
        except Exception as exc:
            self.echo_message(f"Couldn't open {env_path}: {exc}")

    def show_paths_command(self, _source, _args):
        self.echo_message(f"Discord bridge paths | socket={self.socket_path} | events={self.events_path} | state={self.state_path} | env={self._bot_env_path()}")

    def _load_state(self):
        try:
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return
        except Exception as exc:
            self.log(f"Discord bridge state load failed: {exc}")
            return

        pending = data.get("pending", {})
        active = data.get("active", {})
        with self._lock:
            self._pending.clear()
            self._pending.update({key: deque(values) for key, values in pending.items()})
            self._active = dict(active)

    @staticmethod
    def _is_terminal_download_status(status: str) -> bool:
        return str(status or "").strip().lower() in {
            "finished",
            "cancelled",
            "canceled",
            "aborted",
            "failed",
            "error",
            "file not shared",
            "not shared",
            "removed",
        }

    def _prune_stale_state(self):
        stale_keys: list[str] = []
        with self._lock:
            keys = list(self._pending.keys())
        for key in keys:
            user, virtual_path = self._split_key(key)
            transfer = self._find_download_transfer(user, virtual_path)
            status = str(getattr(transfer, "status", "") or "") if transfer is not None else ""
            if transfer is None or self._is_terminal_download_status(status):
                stale_keys.append(key)
        if not stale_keys:
            return
        with self._lock:
            changed = False
            for key in stale_keys:
                if key in self._pending:
                    self._pending.pop(key, None)
                    changed = True
                if key in self._active:
                    self._active.pop(key, None)
                    changed = True
            if changed:
                self._save_state()
        self.log(f"Discord bridge pruned {len(stale_keys)} stale transfer(s) from persisted state")

    def _save_state(self):
        with self._lock:
            data = {
                "pending": {key: list(values) for key, values in self._pending.items()},
                "active": dict(self._active),
            }
        try:
            self.state_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        except Exception as exc:
            self.log(f"Discord bridge state save failed: {exc}")

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _human_size(num_bytes: int) -> str:
        num = float(num_bytes or 0)
        units = ["B", "KiB", "MiB", "GiB", "TiB"]
        for unit in units:
            if num < 1024.0 or unit == units[-1]:
                return f"{int(num)} B" if unit == "B" else f"{num:.1f} {unit}"
            num /= 1024.0
        return f"{num_bytes} B"

    @staticmethod
    def _key(user: str, virtual_path: str) -> str:
        return f"{user}\0{virtual_path}"

    def _append_event(self, payload: dict):
        payload = dict(payload)
        payload.setdefault("ts", self._now_iso())
        try:
            with self.events_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
        except Exception as exc:
            self.log(f"Discord bridge event write failed: {exc}")

    def _search_page(self, token):
        ui = getattr(getattr(self.core, "search", None), "ui_callback", None)
        pages = getattr(ui, "pages", None)
        return pages.get(token) if pages else None

    def _clear_search_session(self, request_id: str) -> bool:
        session = self._search_sessions.pop(str(request_id or "").strip(), None)
        if not session:
            return False
        token = session.get("token")
        ui = getattr(getattr(self.core, "search", None), "ui_callback", None)
        if ui is None:
            return False
        try:
            frame = getattr(ui, "frame", None)
            entry = getattr(frame, "search_entry", None)
            if entry is not None:
                entry.set_text("")
        except Exception:
            pass
        try:
            remove_search = getattr(ui, "remove_search", None)
            if callable(remove_search) and token is not None:
                remove_search(token)
                return True
        except Exception:
            return False
        return False

    def _browse_page(self, user):
        ui = getattr(getattr(self.core, "userbrowse", None), "ui_callback", None)
        pages = getattr(ui, "pages", None)
        return pages.get(user) if pages else None

    def _split_virtual_path(self, value: str) -> list[str]:
        return [part for part in str(value or "").split("\\") if part]

    @staticmethod
    def _clean_name(value: str) -> str:
        return re.sub(r"\s+", " ", str(value or "").replace("_", " ")).strip(" -_[](){}")

    def _looks_like_username(self, value: str, user: str = "") -> bool:
        name = self._clean_name(value)
        lowered = name.lower()
        user_lower = str(user or "").strip().lower()
        compact = re.sub(r"[^a-z0-9]+", "", lowered)
        if not name:
            return True
        if user_lower and (lowered == user_lower or compact == re.sub(r"[^a-z0-9]+", "", user_lower)):
            return True
        if lowered in GENERIC_FOLDER_NAMES:
            return True
        if any(token in lowered for token in ("shared by", "user ", " user's", "files of", "upload from")):
            return True
        if re.fullmatch(r"(?i)(disc|disk|cd)\s*\d+", lowered):
            return True
        if re.search(r"\d{3,}", compact):
            return True
        if re.fullmatch(r"[a-z0-9_.-]+", compact) and len(compact) >= 8 and lowered == compact:
            return True
        return False

    def _album_from_parts(self, parts: list[str]) -> tuple[str, list[str]]:
        if not parts:
            return "<root>", []
        leaf = self._clean_name(parts[-1]) or "<root>"
        if re.fullmatch(r"(?i)(disc|disk|cd)\s*\d+", leaf) and len(parts) >= 2:
            return f"{self._clean_name(parts[-2])} [{leaf}]", parts[:-2]
        return leaf, parts[:-1]

    def _artist_album_from_folder(self, folder: str, user: str = "") -> tuple[str, str]:
        parts = self._split_virtual_path(folder)
        album, prefix_parts = self._album_from_parts(parts)
        match = re.match(r"(?P<artist>.+?)\s*[-–—]\s*(?P<album>.+)", album)
        if match:
            artist = self._clean_name(match.group("artist"))
            parsed_album = self._clean_name(match.group("album"))
            if artist and parsed_album:
                return artist, parsed_album
        candidates = [self._clean_name(part) for part in prefix_parts if not self._looks_like_username(part, user=user)]
        artist = candidates[-1] if candidates else ""
        return artist, album

    @staticmethod
    def _file_extension(filename: str) -> str:
        name = str(filename or "")
        return name.rsplit(".", 1)[-1].strip().lower() if "." in name else ""

    def _format_summary(self, counts: dict[str, int]) -> str:
        if not counts:
            return "unknown audio"
        ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
        labels = [ext.upper() if ext else "unknown" for ext, _count in ranked[:2]]
        return labels[0] if len(labels) == 1 else " + ".join(labels)

    def _summarize_search_rows(self, rows, *, offset: int = 0, limit: int | None = None) -> tuple[list[dict], int]:
        groups = {}
        for row in rows or []:
            try:
                user = str(row[1])
                fullpath = str(row[11])
                size = self._safe_int(row[13], 0)
            except Exception:
                continue
            if not fullpath:
                continue
            folder, _, filename = fullpath.rpartition("\\")
            filename = filename or fullpath
            artist, album = self._artist_album_from_folder(folder, user=user)
            item = groups.setdefault(
                (user, folder),
                {
                    "user": user,
                    "folder": folder,
                    "artist": artist,
                    "album": album,
                    "match_count": 0,
                    "audio_file_count": 0,
                    "size_bytes": 0,
                    "sample_files": [],
                    "visible_files": [],
                    "format_counts": {},
                },
            )
            item["match_count"] += 1
            item["size_bytes"] += size
            ext = self._file_extension(filename)
            if ext in AUDIO_EXTENSIONS:
                item["audio_file_count"] += 1
                item["format_counts"][ext] = item["format_counts"].get(ext, 0) + 1
            if len(item["sample_files"]) < 3:
                item["sample_files"].append(filename)
            item["visible_files"].append({
                "name": filename,
                "fullpath": fullpath,
                "size_bytes": size,
                "size_human": self._human_size(size),
            })
        results = list(groups.values())
        results.sort(key=lambda item: (-item["audio_file_count"], -item["match_count"], -item["size_bytes"], item["folder"].lower(), item["user"].lower()))
        total = len(results)
        start = max(0, self._safe_int(offset, 0))
        end = total if limit is None else max(start, start + max(0, self._safe_int(limit, 0)))
        sliced = results[start:end]
        for item in sliced:
            item["size_human"] = self._human_size(item["size_bytes"])
            item["display_file_count"] = item["audio_file_count"] or item["match_count"]
            item["format_summary"] = self._format_summary(item.pop("format_counts", {}))
        return sliced, total

    def _iter_share_files(self, shares, folder: str = "", recursive: bool = False):
        for share_folder, file_list in (shares or {}).items():
            share_folder = str(share_folder)
            if folder:
                if recursive:
                    if share_folder != folder and not share_folder.startswith(folder + "\\"):
                        continue
                elif share_folder != folder:
                    continue
            for file_data in file_list or []:
                try:
                    filename = str(file_data[1])
                    size = self._safe_int(file_data[2], 0)
                except Exception:
                    continue
                fullpath = f"{share_folder}\\{filename}" if share_folder else filename
                yield {
                    "folder": share_folder,
                    "name": filename,
                    "fullpath": fullpath,
                    "size_bytes": size,
                    "size_human": self._human_size(size),
                }

    def _browse_files_for_folder(self, shares, folder: str) -> list[dict]:
        files = list(self._iter_share_files(shares, folder=folder, recursive=False))
        if not folder and not files:
            files = list(self._iter_share_files(shares))
        files.sort(key=lambda item: item["fullpath"].lower())
        return files

    @staticmethod
    def _virtual_path_in_folder(virtual_path: str, folder: str) -> bool:
        virtual_path = str(virtual_path or "")
        folder = str(folder or "").rstrip("\\")
        if not virtual_path:
            return False
        if not folder:
            return True
        return virtual_path == folder or virtual_path.startswith(folder + "\\")

    def _folder_total_files_from_real_path(self, real_path: str) -> int:
        try:
            parent = Path(str(real_path or "")).expanduser().resolve().parent
        except Exception:
            return 0
        try:
            return sum(1 for entry in parent.iterdir() if entry.is_file())
        except Exception:
            return 0

    def _folder_label(self, folder: str, user: str = "") -> str:
        artist, album = self._artist_album_from_folder(folder, user=user)
        if artist and artist.lower() != album.lower():
            return f"{artist} — {album}"
        return album or "<root>"

    def _upload_event_payload(self, event_name: str, user, virtual_path, real_path) -> dict:
        folder, _, filename = str(virtual_path or "").rpartition("\\")
        return {
            "event": event_name,
            "user": user,
            "path": virtual_path,
            "real_path": real_path,
            "folder": folder,
            "file": filename or str(virtual_path or ""),
            "folder_label": self._folder_label(folder, user=user),
            "folder_total_files": self._folder_total_files_from_real_path(real_path),
        }

    def _resolve_browse_session(self, request_id: str):
        session = self._browse_sessions.get(request_id)
        if not session:
            return None, {"ok": False, "error": f"unknown browse request_id: {request_id}"}
        page = self._browse_page(session["user"])
        if page is None:
            return session, {"ok": False, "ready": False, "error": "browse page not created yet"}
        shares = getattr(page, "shares", None)
        if not shares:
            return session, {"ok": True, "ready": False, "user": session["user"], "folder": session["folder"], "files": []}
        session["shares"] = shares
        return session, {
            "ok": True,
            "ready": True,
            "user": session["user"],
            "folder": session["folder"],
            "files": self._browse_files_for_folder(shares, session["folder"]),
        }

    def _queue_file(
        self,
        user: str,
        virtual_path: str,
        dest: str = "",
        request_id: str | None = None,
        request_group_id: str = "",
    ) -> str:
        request_id = (request_id or str(uuid.uuid4())).strip()
        key = self._key(user, virtual_path)
        with self._lock:
            self._pending[key].append(request_id)
            self._save_state()
        self.core.transfers.get_file(user, virtual_path, dest)
        payload = {"event": "queued", "request_id": request_id, "user": user, "path": virtual_path, "dest": dest}
        if request_group_id:
            payload["request_group_id"] = request_group_id
        self._append_event(payload)
        return request_id

    def _queue_entry(
        self,
        user: str,
        virtual_path: str,
        dest: str = "",
        request_id: str | None = None,
        request_group_id: str = "",
    ) -> dict:
        request_id = self._queue_file(
            user,
            virtual_path,
            dest=dest,
            request_id=request_id,
            request_group_id=request_group_id,
        )
        return {"request_id": request_id, "user": user, "path": virtual_path, "dest": dest}

    def _retry_download(self, user: str, virtual_path: str, dest: str = "", request_id: str | None = None) -> dict:
        request_id = (request_id or str(uuid.uuid4())).strip()
        transfer = self._find_download_transfer(user, virtual_path)
        if transfer is None:
            raise ValueError("existing transfer not found")
        if dest:
            try:
                transfer.path = os.path.abspath(os.path.expanduser(dest))
            except Exception:
                pass
        key = self._key(user, virtual_path)
        with self._lock:
            queue = self._pending[key]
            if request_id not in queue:
                queue.append(request_id)
            self._active.pop(key, None)
            self._save_state()
        self.core.transfers.retry_download(transfer)
        return {
            "request_id": request_id,
            "user": user,
            "path": virtual_path,
            "dest": str(getattr(transfer, "path", "") or dest),
            "mode": "retried",
        }

    def _track_existing_transfer(self, request_group_id: str, user: str, virtual_path: str, dest: str = "") -> str:
        request_id = str(uuid.uuid4()).strip()
        key = self._key(user, virtual_path)
        with self._lock:
            if key in self._active or self._pending.get(key):
                return ""
            self._pending[key].append(request_id)
            self._save_state()
        self._append_event({
            "event": "queued",
            "request_id": request_id,
            "request_group_id": request_group_id,
            "user": user,
            "path": virtual_path,
            "dest": dest,
        })
        return request_id

    def _discover_folder_download_browse_sessions(self) -> None:
        now = time.monotonic()
        for request_group_id, session in list(self._folder_download_sessions.items()):
            user = str(session.get("user") or "")
            folder = str(session.get("folder") or "")
            dest = str(session.get("dest") or "")
            known_paths = session.setdefault("known_paths", set())
            page = self._browse_page(user)
            shares = getattr(page, "shares", None) if page is not None else None
            if shares:
                for item in self._iter_share_files(shares, folder=folder, recursive=False):
                    virtual_path = item["fullpath"]
                    if virtual_path in known_paths:
                        continue
                    known_paths.add(virtual_path)
                    item_dest = self._folder_download_dest(user, item["folder"], root_folder=folder, dest=dest)
                    self._queue_entry(
                        user,
                        virtual_path,
                        dest=item_dest,
                        request_group_id=request_group_id,
                    )
                session["last_seen"] = now
                session["browse_completed"] = True
                continue
            if page is not None:
                session["last_seen"] = now
                continue
            if now - float(session.get("last_seen") or now) > 900:
                self._folder_download_sessions.pop(request_group_id, None)

    def _folder_download_dest(self, user: str, folder: str, root_folder: str = "", dest: str = "") -> str:
        folder = str(folder or "")
        root_folder = str(root_folder or folder)
        if dest:
            base_dir = os.path.abspath(os.path.expanduser(dest))
        else:
            base_dir = self.core.transfers.get_default_download_folder(user)
        album_name = self._clean_name(self._split_virtual_path(root_folder)[-1] if root_folder else "")
        album_name = album_name or self._clean_name(self._split_virtual_path(folder)[-1] if folder else "") or "album"
        return os.path.join(base_dir, album_name)

    def _queue_folder(self, user: str, folder: str, shares, dest: str = "") -> list[dict]:
        queued = []
        for item in self._iter_share_files(shares, folder=folder, recursive=True):
            item_dest = self._folder_download_dest(user, item["folder"], root_folder=folder, dest=dest)
            queued.append(self._queue_entry(user, item["fullpath"], dest=item_dest))
        return queued

    def _resolve_files_by_names(self, shares, folder: str, names) -> list[dict]:
        wanted = {str(name) for name in names or []}
        return [
            item for item in self._iter_share_files(shares, folder=folder, recursive=False)
            if item["name"] in wanted or item["fullpath"] in wanted
        ]

    @staticmethod
    def _split_key(key: str) -> tuple[str, str]:
        user, _sep, virtual_path = str(key).partition("\0")
        return user, virtual_path

    def _find_download_transfer(self, user: str, virtual_path: str):
        transfers = getattr(getattr(self.core, "transfers", None), "downloads", None) or []
        for download in list(transfers):
            if str(getattr(download, "user", "") or "") != user:
                continue
            if str(getattr(download, "filename", "") or "") != virtual_path:
                continue
            return download
        return None

    def _queue_entries(self) -> list[dict]:
        self._prune_stale_state()
        with self._lock:
            pending_snapshot = {key: list(values) for key, values in self._pending.items()}
            active_snapshot = dict(self._active)
        entries = []
        for key, request_ids in pending_snapshot.items():
            user, virtual_path = self._split_key(key)
            transfer = self._find_download_transfer(user, virtual_path)
            active_request_id = active_snapshot.get(key)
            status = str(getattr(transfer, "status", "Queued") or "Queued") if transfer is not None else "Queued"
            current = self._safe_int(getattr(transfer, "current_byte_offset", 0), 0) if transfer is not None else 0
            total = self._safe_int(getattr(transfer, "size", 0), 0) if transfer is not None else 0
            percent = max(0, min(100, int((current * 100) / total))) if total > 0 and current > 0 else 0
            queue_position = self._safe_int(getattr(transfer, "queue_position", 0), 0) if transfer is not None else 0
            for index, request_id in enumerate(request_ids, start=1):
                is_active = request_id == active_request_id
                item_status = status if is_active else "Queued"
                entries.append({
                    "request_id": request_id,
                    "user": user,
                    "path": virtual_path,
                    "status": item_status,
                    "active": is_active,
                    "queue_index": index,
                    "queue_depth": len(request_ids),
                    "queue_position": queue_position,
                    "current_bytes": current if is_active else 0,
                    "total_bytes": total if is_active else 0,
                    "percent": percent if is_active else 0,
                })
        entries.sort(key=lambda item: (0 if item["active"] else 1, item["user"].lower(), item["path"].lower(), item["request_id"]))
        return entries

    def _resolve_request_entry(self, request_token: str):
        token = str(request_token or "").strip()
        if not token:
            return None, "request_id is required"
        entries = self._queue_entries()
        exact = [entry for entry in entries if entry["request_id"] == token]
        if len(exact) == 1:
            return exact[0], None
        prefix = [entry for entry in entries if entry["request_id"].startswith(token)]
        if not prefix:
            return None, f"unknown request_id: {token}"
        if len(prefix) > 1:
            choices = ", ".join(entry["request_id"][:8] for entry in prefix[:5])
            more = "…" if len(prefix) > 5 else ""
            return None, f"request_id prefix is ambiguous: {choices}{more}"
        return prefix[0], None

    def _remove_entry(self, entry: dict, *, reason: str = "Removed from queue") -> dict:
        user = entry["user"]
        virtual_path = entry["path"]
        request_id = entry["request_id"]
        key = self._key(user, virtual_path)
        transfer = self._find_download_transfer(user, virtual_path)
        if transfer is not None and getattr(transfer, "status", "") != "Finished":
            try:
                transfer.status = "Cancelled"
                self.core.transfers.abort_transfer(transfer)
            except Exception:
                pass
            try:
                if transfer in self.core.transfers.downloads:
                    self.core.transfers.downloads.remove(transfer)
                    if self.core.transfers.downloadsview:
                        self.core.transfers.downloadsview.remove_specific(transfer, True)
            except Exception:
                pass
        with self._lock:
            queue = self._pending.get(key)
            if queue and request_id in queue:
                queue.remove(request_id)
                if not queue:
                    self._pending.pop(key, None)
            if self._active.get(key) == request_id:
                self._active.pop(key, None)
            self._save_state()
        self._progress_marks.pop(request_id, None)
        self._append_event({"event": "removed", "request_id": request_id, "user": user, "path": virtual_path, "reason": reason})
        return entry

    def _remove_download_request(self, request_token: str) -> dict:
        entry, error = self._resolve_request_entry(request_token)
        if entry is None:
            return {"ok": False, "error": error}
        self._remove_entry(entry)
        request_id = entry["request_id"]
        return {"ok": True, "removed": [entry], "message": f"Removed {request_id[:8]} from queue"}

    def _remove_download_matching(self, *, user: str = "", path_contains: str = "") -> dict:
        user_token = str(user or "").strip().lower()
        path_token = str(path_contains or "").strip().lower()
        if not user_token and not path_token:
            return {"ok": False, "error": "provide user and/or path_contains"}
        matched = [
            entry for entry in self._queue_entries()
            if (not user_token or str(entry.get("user") or "").lower() == user_token)
            and (not path_token or path_token in str(entry.get("path") or "").lower())
        ]
        if not matched:
            target = user or path_contains
            return {"ok": False, "error": f"no queued downloads matched: {target}"}
        removed = [self._remove_entry(entry, reason="Bulk removed from queue") for entry in matched]
        summary = ", ".join(bit for bit in (f"user={user}" if user_token else "", f"path~={path_contains}" if path_token else "") if bit)
        return {"ok": True, "removed": removed, "message": f"Removed {len(removed)} queue item(s) matching {summary}"}

    def _start_server(self):
        if self._server_thread and self._server_thread.is_alive():
            return
        self._stop.clear()
        try:
            self.socket_path.unlink(missing_ok=True)
        except Exception:
            pass
        self._server_thread = threading.Thread(target=self._serve, name="DiscordBridgeSocket", daemon=True)
        self._server_thread.start()

    def _start_progress_watcher(self):
        if self._progress_thread and self._progress_thread.is_alive():
            return
        self._progress_thread = threading.Thread(target=self._watch_download_progress, name="DiscordBridgeProgress", daemon=True)
        self._progress_thread.start()

    def _watch_download_progress(self):
        while not self._stop.wait(2.0):
            try:
                self._poll_download_progress()
            except Exception:
                continue

    def _poll_download_progress(self):
        transfers = getattr(getattr(self.core, "transfers", None), "downloads", None) or []
        self._discover_folder_download_browse_sessions()
        for download in list(transfers):
            user = str(getattr(download, "user", "") or "")
            virtual_path = str(getattr(download, "filename", "") or "")
            if not user or not virtual_path:
                continue
            request_id = self._claim_request_id(user, virtual_path, finish=False)
            if not request_id:
                continue
            status = str(getattr(download, "status", "") or "")
            if status == "Transferring":
                size = self._safe_int(getattr(download, "size", 0), 0)
                current = self._safe_int(getattr(download, "current_byte_offset", 0), 0)
                if size <= 0 or current <= 0:
                    continue
                percent = max(0, min(99, int((current * 100) / size)))
                marks = self._progress_marks.setdefault(request_id, set())
                newly_reached = [threshold for threshold in (25, 50, 75) if percent >= threshold and threshold not in marks]
                if not newly_reached:
                    continue
                highest = max(newly_reached)
                for threshold in (25, 50, 75):
                    if threshold <= highest:
                        marks.add(threshold)
                self._append_event({
                    "event": "progress",
                    "request_id": request_id,
                    "user": user,
                    "path": virtual_path,
                    "percent": highest,
                    "current_bytes": current,
                    "total_bytes": size,
                })
                continue
            if status in {"Queued", "Getting status", "Paused", "Filtered", "Finished"}:
                continue
            if request_id in self._progress_marks and status in {"Cancelled"}:
                continue
            self._progress_marks.pop(request_id, None)
            self._append_event({
                "event": "error",
                "request_id": request_id,
                "user": user,
                "path": virtual_path,
                "error": status or "download failed",
            })
            self._claim_request_id(user, virtual_path, finish=True)

    def _serve(self):
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._server_socket = server
        try:
            server.bind(str(self.socket_path))
            try:
                owner = self.base_dir.stat()
                os.chown(self.socket_path, owner.st_uid, owner.st_gid)
            except Exception as exc:
                self.log(f"Discord bridge socket ownership fix failed: {exc}")
            os.chmod(self.socket_path, 0o660)
            server.listen(5)
            server.settimeout(1.0)
            while not self._stop.is_set():
                try:
                    client, _ = server.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break
                threading.Thread(target=self._handle_client, args=(client,), daemon=True).start()
        finally:
            try:
                server.close()
                self.socket_path.unlink(missing_ok=True)
            except Exception:
                pass

    def _handle_client(self, client: socket.socket):
        with client:
            try:
                chunks = []
                while True:
                    data = client.recv(65536)
                    if not data:
                        break
                    chunks.append(data)
                if not chunks:
                    return
                request = json.loads(b"".join(chunks).decode("utf-8"))
                response = self._run_on_main_thread(request)
            except Exception as exc:
                response = {"ok": False, "error": str(exc)}
            try:
                client.sendall((json.dumps(response, ensure_ascii=False) + "\n").encode("utf-8"))
            except Exception:
                pass

    def _run_on_main_thread(self, request: dict) -> dict:
        result = {}
        done = threading.Event()

        def runner():
            try:
                result.update(self._handle_request(request))
            except Exception as exc:
                result.update({"ok": False, "error": str(exc)})
            finally:
                done.set()
            return False

        GLib.idle_add(runner)
        if not done.wait(30):
            return {"ok": False, "error": "timeout waiting for Nicotine main loop"}
        return result

    def _handle_request(self, request: dict) -> dict:
        op = str(request.get("op") or "").strip().lower()

        if op == "ping":
            return {"ok": True, "message": "pong"}

        if op == "search":
            query = str(request.get("query") or "").strip()
            if not query:
                return {"ok": False, "error": "query is required"}
            request_id = str(request.get("request_id") or uuid.uuid4()).strip()
            self.core.search.do_search(query, "global")
            self._search_sessions[request_id] = {"token": self.core.search.token, "query": query}
            return {"ok": True, "request_id": request_id, "message": "search started"}

        if op == "search_results":
            request_id = str(request.get("request_id") or "").strip()
            session = self._search_sessions.get(request_id)
            if not session:
                return {"ok": False, "error": f"unknown search request_id: {request_id}"}
            page = self._search_page(session["token"])
            rows = list(getattr(page, "all_data", []) or []) if page else []
            offset = self._safe_int(request.get("offset"), 0)
            requested_limit = request.get("limit")
            limit = self._safe_int(requested_limit, self._setting_limit("results_limit", 5)) if requested_limit not in (None, "") else self._setting_limit("results_limit", 5)
            limit = max(1, min(100, limit))
            results, total = self._summarize_search_rows(rows, offset=offset, limit=limit)
            return {
                "ok": True,
                "ready": bool(page),
                "query": session["query"],
                "results": results,
                "total_results": total,
                "offset": offset,
                "limit": limit,
            }

        if op == "clear_search":
            request_id = str(request.get("request_id") or "").strip()
            if not request_id:
                return {"ok": False, "error": "request_id is required"}
            cleared = self._clear_search_session(request_id)
            return {"ok": True, "cleared": cleared, "request_id": request_id}

        if op == "browse_folder":
            user = str(request.get("user") or "").strip()
            folder = str(request.get("folder") or "").strip()
            if not user:
                return {"ok": False, "error": "user is required"}
            request_id = str(request.get("request_id") or uuid.uuid4()).strip()
            self._browse_sessions[request_id] = {"user": user, "folder": folder, "shares": None}
            self.core.userbrowse.browse_user(user, path=folder or None, new_request=True, switch_page=False)
            return {"ok": True, "request_id": request_id, "message": "browse started"}

        if op == "browse_folder_results":
            return self._resolve_browse_session(str(request.get("request_id") or "").strip())[1]

        if op == "download":
            user = str(request.get("user") or "").strip()
            path = str(request.get("path") or "").strip()
            dest = str(request.get("dest") or "").strip()
            if not user or not path:
                return {"ok": False, "error": "user and path are required"}
            request_id = self._queue_file(user, path, dest=dest)
            return {"ok": True, "queued": 1, "request_ids": [request_id], "entries": [{"request_id": request_id, "user": user, "path": path, "dest": dest}]}

        if op == "retry_download":
            user = str(request.get("user") or "").strip()
            path = str(request.get("path") or "").strip()
            dest = str(request.get("dest") or "").strip()
            request_id = str(request.get("request_id") or uuid.uuid4()).strip()
            if not user or not path:
                return {"ok": False, "error": "user and path are required"}
            try:
                entry = self._retry_download(user, path, dest=dest, request_id=request_id)
            except ValueError as exc:
                return {"ok": False, "error": str(exc)}
            return {
                "ok": True,
                "retried": 1,
                "request_ids": [entry["request_id"]],
                "entries": [entry],
            }

        if op == "download_folder":
            session, payload = self._resolve_browse_session(str(request.get("request_id") or "").strip())
            if not session or not payload.get("ready"):
                return payload
            queued = self._queue_folder(session["user"], session["folder"], session["shares"], dest=str(request.get("dest") or "").strip())
            return {"ok": True, "queued": len(queued), "request_ids": [item["request_id"] for item in queued], "entries": queued}

        if op == "download_search_folder":
            user = str(request.get("user") or "").strip()
            folder = str(request.get("folder") or "").strip()
            dest = str(request.get("dest") or "").strip()
            request_group_id = str(request.get("request_group_id") or uuid.uuid4()).strip()
            visible_files = request.get("visible_files") or request.get("files") or []
            if not user or not folder:
                return {"ok": False, "error": "user and folder are required"}
            folder_dest = self._folder_download_dest(user, folder, root_folder=folder, dest=dest)
            queued = []
            known_paths = set()
            for item in visible_files:
                fullpath = str((item or {}).get("fullpath") or (item or {}).get("path") or item or "")
                if not fullpath or fullpath in known_paths:
                    continue
                known_paths.add(fullpath)
                queued.append(self._queue_entry(user, fullpath, dest=folder_dest, request_group_id=request_group_id))
            self._folder_download_sessions[request_group_id] = {
                "user": user,
                "folder": folder,
                "dest": dest,
                "known_paths": known_paths,
                "last_seen": time.monotonic(),
            }
            self.core.userbrowse.browse_user(user, path=folder or None, new_request=True, switch_page=False)
            return {
                "ok": True,
                "queued": len(queued),
                "request_ids": [item["request_id"] for item in queued],
                "entries": queued,
                "request_group_id": request_group_id,
                "waiting_for_folder_contents": True,
            }

        if op == "download_files":
            session, payload = self._resolve_browse_session(str(request.get("request_id") or "").strip())
            if not session or not payload.get("ready"):
                return payload
            resolved = self._resolve_files_by_names(session["shares"], session["folder"], request.get("files") or [])
            dest = str(request.get("dest") or "").strip()
            queued = [
                self._queue_entry(
                    session["user"],
                    item["fullpath"],
                    dest=self._folder_download_dest(session["user"], item.get("folder", session["folder"]), root_folder=session["folder"], dest=dest),
                )
                for item in resolved
            ]
            return {"ok": True, "queued": len(queued), "request_ids": [item["request_id"] for item in queued], "entries": queued, "files": resolved}

        if op == "queue_list":
            return {"ok": True, "entries": self._queue_entries()}

        if op == "queue_remove":
            return self._remove_download_request(str(request.get("request_id") or request.get("id") or "").strip())

        if op == "queue_remove_matching":
            return self._remove_download_matching(
                user=str(request.get("user") or "").strip(),
                path_contains=str(request.get("path_contains") or request.get("path") or "").strip(),
            )

        return {"ok": False, "error": f"unknown op: {op or '<empty>'}"}

    def _claim_request_id(self, user: str, virtual_path: str, finish: bool = False):
        key = self._key(user, virtual_path)
        with self._lock:
            if finish:
                request_id = self._active.pop(key, None)
                changed = request_id is not None
                queue = self._pending.get(key)
                if queue and request_id is None:
                    request_id = queue.popleft()
                    changed = True
                elif queue and request_id is not None:
                    if queue and queue[0] == request_id:
                        queue.popleft()
                    elif request_id in queue:
                        queue.remove(request_id)
                    changed = True
                if queue is not None and not queue:
                    self._pending.pop(key, None)
                if changed:
                    self._save_state()
                return request_id
            request_id = self._active.get(key)
            if request_id is not None:
                return request_id
            queue = self._pending.get(key)
            if not queue:
                return None
            request_id = queue[0]
            self._active[key] = request_id
            self._save_state()
            return request_id

    def download_started_notification(self, user, virtual_path, real_path):
        request_id = self._claim_request_id(user, virtual_path, finish=False)
        if request_id:
            self._progress_marks.pop(request_id, None)
        if not self.settings.get("emit_download_started", True):
            return
        self._append_event({
            "event": "started",
            "request_id": request_id,
            "user": user,
            "path": virtual_path,
            "real_path": real_path,
        })

    def download_finished_notification(self, user, virtual_path, real_path):
        request_id = self._claim_request_id(user, virtual_path, finish=True)
        if request_id is None:
            request_id = self._claim_request_id(user, virtual_path, finish=False)
        if request_id:
            self._progress_marks.pop(request_id, None)
        if not self.settings.get("emit_download_finished", True):
            return
        self._append_event({
            "event": "finished",
            "request_id": request_id,
            "user": user,
            "path": virtual_path,
            "real_path": real_path,
        })

    def upload_started_notification(self, user, virtual_path, real_path):
        if not self.settings.get("emit_upload_started", True):
            return
        self._append_event(self._upload_event_payload("upload_started", user, virtual_path, real_path))

    def upload_finished_notification(self, user, virtual_path, real_path):
        if not self.settings.get("emit_upload_finished", True):
            return
        self._append_event(self._upload_event_payload("upload_finished", user, virtual_path, real_path))

    def _shutdown(self):
        self._stop.set()
        try:
            if self._server_socket:
                self._server_socket.close()
            self.socket_path.unlink(missing_ok=True)
        except Exception:
            pass
        self._server_socket = None
        self._save_state()
