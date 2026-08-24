import importlib.util
import sys
import threading
import time
import types
import pytest
from collections import defaultdict, deque
from pathlib import Path


class DummyBasePlugin:
    def __init__(self, *args, **kwargs):
        pass

    def log(self, message):
        self.last_log = message


def load_plugin_module():
    gi = types.ModuleType("gi")
    gi_repository = types.ModuleType("gi.repository")
    gi_repository.GLib = types.SimpleNamespace()
    pynicotine = types.ModuleType("pynicotine")
    pluginsystem = types.ModuleType("pynicotine.pluginsystem")
    pluginsystem.BasePlugin = DummyBasePlugin
    sys.modules.setdefault("gi", gi)
    sys.modules.setdefault("gi.repository", gi_repository)
    sys.modules.setdefault("pynicotine", pynicotine)
    sys.modules.setdefault("pynicotine.pluginsystem", pluginsystem)

    path = Path(__file__).parent / "discord_bridge" / "__init__.py"
    spec = importlib.util.spec_from_file_location("discord_bridge_plugin_for_tests", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def make_plugin(tmp_path, transfers):
    module = load_plugin_module()
    plugin = module.Plugin.__new__(module.Plugin)
    plugin._lock = threading.RLock()
    plugin._pending = defaultdict(deque)
    plugin._active = {}
    plugin._progress_marks = {}
    plugin._events_lock = threading.Lock()
    plugin._started_at = time.monotonic() - 125
    plugin._stop = threading.Event()
    plugin._server_thread = None
    plugin._server_socket = None
    plugin._server_ready = threading.Event()
    plugin._server_error = ""
    plugin._last_progress_error_log = 0.0
    plugin.base_dir = tmp_path
    plugin.socket_path = tmp_path / "control.sock"
    plugin.events_path = tmp_path / "events.jsonl"
    plugin.state_path = tmp_path / "state.json"
    plugin.core = types.SimpleNamespace(
        transfers=types.SimpleNamespace(downloads=transfers),
        protothread=types.SimpleNamespace(server_disconnected=False),
    )
    plugin.last_log = ""
    plugin.log = lambda message: setattr(plugin, "last_log", message)
    return plugin


def test_queue_list_prunes_queued_transfer_when_target_file_already_exists(tmp_path):
    target_dir = tmp_path / "Soulseek" / "Album"
    target_dir.mkdir(parents=True)
    downloaded_file = target_dir / "01 Song.flac"
    downloaded_file.write_bytes(b"already downloaded")

    transfer = types.SimpleNamespace(
        user="alice",
        filename="music\\Artist\\Album\\01 Song.flac",
        path=str(target_dir),
        status="Queued",
        current_byte_offset=0,
        size=123,
        queue_position=0,
    )
    plugin = make_plugin(tmp_path, [transfer])
    key = plugin._key("alice", "music\\Artist\\Album\\01 Song.flac")
    plugin._pending[key].append("request-1")
    plugin._active[key] = "request-1"

    entries = plugin._queue_entries()

    assert entries == []
    assert dict(plugin._pending) == {}
    assert plugin._active == {}
    assert plugin.state_path.read_text(encoding="utf-8") == '{\n  "pending": {},\n  "active": {}\n}'


def test_queue_list_keeps_queued_transfer_when_target_file_is_not_present(tmp_path):
    target_dir = tmp_path / "Soulseek" / "Album"
    target_dir.mkdir(parents=True)
    transfer = types.SimpleNamespace(
        user="alice",
        filename="music\\Artist\\Album\\02 Missing.flac",
        path=str(target_dir),
        status="Queued",
        current_byte_offset=0,
        size=123,
        queue_position=7,
    )
    plugin = make_plugin(tmp_path, [transfer])
    key = plugin._key("alice", "music\\Artist\\Album\\02 Missing.flac")
    plugin._pending[key].append("request-2")

    entries = plugin._queue_entries()

    assert len(entries) == 1
    assert entries[0]["request_id"] == "request-2"
    assert entries[0]["status"] == "Queued"


def test_status_reports_soulseek_and_tracked_request_health(tmp_path):
    plugin = make_plugin(tmp_path, [types.SimpleNamespace()])
    plugin._pending[plugin._key("alice", "music\\song.flac")].extend(["request-1", "request-2"])
    plugin._active[plugin._key("alice", "music\\song.flac")] = "request-1"

    status = plugin._handle_request({"op": "status"})

    assert status["ok"] is True
    assert status["soulseek_connected"] is True
    assert status["pending_requests"] == 1
    assert status["active_requests"] == 1
    assert status["download_transfers"] == 1
    assert status["uptime_seconds"] >= 124


def test_event_log_rotates_before_it_grows_without_bound(tmp_path):
    plugin = make_plugin(tmp_path, [])
    module_globals = plugin._append_event.__globals__
    old_limit = module_globals["MAX_EVENT_LOG_BYTES"]
    module_globals["MAX_EVENT_LOG_BYTES"] = 10
    try:
        plugin._append_event({"event": "first"})
        plugin._append_event({"event": "second"})
    finally:
        module_globals["MAX_EVENT_LOG_BYTES"] = old_limit

    rotated = plugin.events_path.with_suffix(".jsonl.1")
    assert rotated.exists()
    assert '"first"' in rotated.read_text(encoding="utf-8")
    assert '"second"' in plugin.events_path.read_text(encoding="utf-8")


def test_expired_main_thread_request_is_not_executed_late(tmp_path):
    plugin = make_plugin(tmp_path, [])
    module_globals = plugin._run_on_main_thread.__globals__
    old_timeout = module_globals["MAIN_THREAD_TIMEOUT_SECONDS"]
    old_glib = module_globals["GLib"]
    queued_callbacks = []
    handled = []
    module_globals["MAIN_THREAD_TIMEOUT_SECONDS"] = 0.01
    module_globals["GLib"] = types.SimpleNamespace(idle_add=queued_callbacks.append)
    plugin._handle_request = lambda request: handled.append(request) or {"ok": True}
    try:
        reply = plugin._run_on_main_thread({"op": "download"})
        queued_callbacks[0]()
    finally:
        module_globals["MAIN_THREAD_TIMEOUT_SECONDS"] = old_timeout
        module_globals["GLib"] = old_glib

    assert reply == {"ok": False, "error": "timeout waiting for Nicotine main loop"}
    assert handled == []


def test_queue_failure_rolls_back_persisted_request(tmp_path):
    plugin = make_plugin(tmp_path, [])

    def fail_get_file(*_args):
        raise RuntimeError("queue failed")

    plugin.core.transfers.get_file = fail_get_file
    with pytest.raises(RuntimeError, match="queue failed"):
        plugin._queue_file("alice", "music\\song.flac", request_id="request-1")

    assert dict(plugin._pending) == {}
    assert plugin._active == {}
    persisted = plugin.state_path.read_text(encoding="utf-8")
    assert '"pending": {}' in persisted


def test_server_reports_ready_only_after_listen_succeeds(tmp_path):
    plugin = make_plugin(tmp_path, [])
    try:
        assert plugin._start_server() is True
        assert plugin.socket_path.exists()
        assert plugin._server_error == ""
    finally:
        plugin._shutdown()
