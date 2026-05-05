from __future__ import annotations

import json
import os
import socket
import subprocess
import threading
import uuid
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

from gi.repository import GLib
from pynicotine.pluginsystem import BasePlugin


class Plugin(BasePlugin):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        base_dir = Path.home() / ".local" / "share" / "nicotine" / "discord-bridge"
        self.base_dir = base_dir
        self.socket_path = base_dir / "control.sock"
        self.events_path = base_dir / "events.jsonl"
        self.state_path = base_dir / "state.json"
        self.runtime_path = base_dir / "runtime.json"
        self.settings = {
            "results_limit": 5,
            "track_picker_limit": 25,
            "emit_upload_started": True,
            "emit_upload_finished": False,
            "emit_download_started": True,
            "emit_download_finished": True,
            "bot_env_path": str(Path.home() / "nicotine-discord-bridge" / ".env"),
        }
        self.metasettings = {
            "results_limit": {
                "description": "How many album/folder results Discord should show per search",
                "type": "integer",
            },
            "track_picker_limit": {
                "description": "How many tracks Discord should show in the picker (max 25)",
                "type": "integer",
            },
            "bot_env_path": {
                "description": "Discord bot .env file path used by /bridgeenv",
                "type": "string",
            },
            "emit_upload_started": {
                "description": "Write upload-start events for Discord alerts",
                "type": "bool",
            },
            "emit_upload_finished": {
                "description": "Write upload-finished events",
                "type": "bool",
            },
            "emit_download_started": {
                "description": "Write download-started events",
                "type": "bool",
            },
            "emit_download_finished": {
                "description": "Write download-finished events",
                "type": "bool",
            },
        }
        self.__privatecommands__ = [
            ("bridgeenv", self.open_env_command),
            ("bridgepaths", self.show_paths_command),
        ]
        self._stop = threading.Event()
        self._server_thread = None
        self._server_socket = None
        self._lock = threading.RLock()
        self._pending = defaultdict(deque)
        self._active = {}
        self._search_sessions = {}
        self._browse_sessions = {}

    def init(self):
        self._ensure_dirs()

    def loaded_notification(self):
        self._ensure_dirs()
        self._load_state()
        self._write_runtime_manifest()
        self._start_server()
        self.log(f"Discord bridge ready at {self.socket_path} (events={self.events_path})")

    def disable(self):
        self._shutdown()

    def shutdown_notification(self):
        self._shutdown()

    def unloaded_notification(self):
        self._shutdown()

    def _ensure_dirs(self):
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _safe_int(self, value, default):
        try:
            return int(value)
        except Exception:
            return default

    def _results_limit(self) -> int:
        return max(1, min(25, self._safe_int(self.settings.get("results_limit"), 5)))

    def _track_picker_limit(self) -> int:
        return max(1, min(25, self._safe_int(self.settings.get("track_picker_limit"), 25)))

    def _bot_env_path(self) -> Path:
        value = str(self.settings.get("bot_env_path") or "").strip()
        if not value:
            value = str(Path.home() / "nicotine-discord-bridge" / ".env")
        return Path(value).expanduser()

    def _runtime_manifest(self) -> dict:
        return {
            "data_dir": str(self.base_dir),
            "socket_path": str(self.socket_path),
            "events_path": str(self.events_path),
            "state_path": str(self.state_path),
            "results_limit": self._results_limit(),
            "track_picker_limit": self._track_picker_limit(),
            "bot_env_path": str(self._bot_env_path()),
            "emit_upload_started": bool(self.settings.get("emit_upload_started", True)),
            "emit_upload_finished": bool(self.settings.get("emit_upload_finished", False)),
            "emit_download_started": bool(self.settings.get("emit_download_started", True)),
            "emit_download_finished": bool(self.settings.get("emit_download_finished", True)),
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
        self.echo_message(
            f"Discord bridge paths | socket={self.socket_path} | events={self.events_path} | state={self.state_path} | env={self._bot_env_path()}"
        )

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
            for key, values in pending.items():
                self._pending[key] = deque(values)
            self._active = dict(active)

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
        try:
            num = float(num_bytes)
        except Exception:
            return "0 B"
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

    def _browse_page(self, user):
        ui = getattr(getattr(self.core, "userbrowse", None), "ui_callback", None)
        pages = getattr(ui, "pages", None)
        return pages.get(user) if pages else None

    def _summarize_search_rows(self, rows) -> list[dict]:
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
            item = groups.setdefault(
                (user, folder),
                {"user": user, "folder": folder, "match_count": 0, "size_bytes": 0, "sample_files": []},
            )
            item["match_count"] += 1
            item["size_bytes"] += size
            if len(item["sample_files"]) < 3:
                item["sample_files"].append(filename)
        results = list(groups.values())
        results.sort(key=lambda item: (-item["match_count"], -item["size_bytes"], item["folder"].lower(), item["user"].lower()))
        for item in results:
            item["size_human"] = self._human_size(item["size_bytes"])
        return results[: self._results_limit()]

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

    def _queue_file(self, user: str, virtual_path: str, dest: str = "", request_id: str | None = None) -> str:
        request_id = (request_id or str(uuid.uuid4())).strip()
        key = self._key(user, virtual_path)
        with self._lock:
            self._pending[key].append(request_id)
            self._save_state()
        self.core.transfers.get_file(user, virtual_path, dest)
        self._append_event({"event": "queued", "request_id": request_id, "user": user, "path": virtual_path, "dest": dest})
        return request_id

    def _queue_folder(self, user: str, folder: str, shares, dest: str = "") -> list[dict]:
        queued = []
        for item in self._iter_share_files(shares, folder=folder, recursive=True):
            queued.append({"request_id": self._queue_file(user, item["fullpath"], dest=dest), "path": item["fullpath"]})
        return queued

    def _resolve_files_by_names(self, shares, folder: str, names) -> list[dict]:
        wanted = {str(name) for name in names or []}
        return [
            item for item in self._iter_share_files(shares, folder=folder, recursive=False)
            if item["name"] in wanted or item["fullpath"] in wanted
        ]

    def _start_server(self):
        if self._server_thread and self._server_thread.is_alive():
            return
        self._stop.clear()
        try:
            if self.socket_path.exists():
                self.socket_path.unlink()
        except Exception:
            pass
        self._server_thread = threading.Thread(target=self._serve, name="DiscordBridgeSocket", daemon=True)
        self._server_thread.start()

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
            except Exception:
                pass
            try:
                if self.socket_path.exists():
                    self.socket_path.unlink()
            except Exception:
                pass

    def _handle_client(self, client: socket.socket):
        with client:
            try:
                data = client.recv(65536)
                if not data:
                    return
                request = json.loads(data.decode("utf-8"))
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
            return {"ok": True, "ready": bool(page), "query": session["query"], "results": self._summarize_search_rows(rows)}

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
            return {"ok": True, "queued": 1, "request_ids": [request_id]}

        if op == "download_folder":
            session, payload = self._resolve_browse_session(str(request.get("request_id") or "").strip())
            if not session or not payload.get("ready"):
                return payload
            queued = self._queue_folder(session["user"], session["folder"], session["shares"], dest=str(request.get("dest") or "").strip())
            return {"ok": True, "queued": len(queued), "request_ids": [item["request_id"] for item in queued]}

        if op == "download_files":
            session, payload = self._resolve_browse_session(str(request.get("request_id") or "").strip())
            if not session or not payload.get("ready"):
                return payload
            resolved = self._resolve_files_by_names(session["shares"], session["folder"], request.get("files") or [])
            queued = [
                {"request_id": self._queue_file(session["user"], item["fullpath"], dest=str(request.get("dest") or "").strip()), "path": item["fullpath"]}
                for item in resolved
            ]
            return {"ok": True, "queued": len(queued), "request_ids": [item["request_id"] for item in queued], "files": resolved}

        return {"ok": False, "error": f"unknown op: {op or '<empty>'}"}

    def _claim_request_id(self, user: str, virtual_path: str, finish: bool = False):
        key = self._key(user, virtual_path)
        with self._lock:
            if finish:
                request_id = self._active.pop(key, None)
                queue = self._pending.get(key)
                if queue and request_id is not None:
                    if queue and queue[0] == request_id:
                        queue.popleft()
                    elif request_id in queue:
                        try:
                            queue.remove(request_id)
                        except ValueError:
                            pass
                    if not queue:
                        self._pending.pop(key, None)
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
        if not self.settings.get("emit_download_started", True):
            return
        self._append_event({
            "event": "started",
            "request_id": self._claim_request_id(user, virtual_path, finish=False),
            "user": user,
            "path": virtual_path,
            "real_path": real_path,
        })

    def download_finished_notification(self, user, virtual_path, real_path):
        request_id = self._claim_request_id(user, virtual_path, finish=True)
        if request_id is None:
            request_id = self._claim_request_id(user, virtual_path, finish=False)
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
        self._append_event({"event": "upload_started", "user": user, "path": virtual_path, "real_path": real_path})

    def upload_finished_notification(self, user, virtual_path, real_path):
        if not self.settings.get("emit_upload_finished", False):
            return
        self._append_event({"event": "upload_finished", "user": user, "path": virtual_path, "real_path": real_path})

    def _shutdown(self):
        self._stop.set()
        try:
            if self._server_socket:
                self._server_socket.close()
        except Exception:
            pass
        self._server_socket = None
        try:
            if self.socket_path.exists():
                self.socket_path.unlink()
        except Exception:
            pass
        self._save_state()
