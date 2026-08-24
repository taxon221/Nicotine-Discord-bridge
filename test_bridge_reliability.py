import json
import socket
import sys
import threading
import time
from pathlib import Path


BOT_DIR = Path(__file__).parent / "bot"
sys.path.insert(0, str(BOT_DIR))

from bridge_state import BridgeState, DownloadBatch, DownloadItem, heal_downloads_present_on_disk, read_event_lines
from bridge_transport import unix_json_call


def test_event_reader_resets_cursor_after_rotation(tmp_path):
    events = tmp_path / "events.jsonl"
    events.write_text('{"event":"old"}\n', encoding="utf-8")
    _lines, old_cursor, reset = read_event_lines(events, 0)
    assert reset is False

    events.write_text('{"event":"new"}\n', encoding="utf-8")
    lines, new_cursor, reset = read_event_lines(events, old_cursor + 100)

    assert reset is True
    assert [json.loads(line)["event"] for line in lines] == ["new"]
    assert new_cursor == events.stat().st_size


def test_event_reader_leaves_partial_final_record_for_next_poll(tmp_path):
    events = tmp_path / "events.jsonl"
    complete = b'{"event":"first"}\n'
    events.write_bytes(complete + b'{"event":"partial"')

    lines, cursor, reset = read_event_lines(events, 0)

    assert reset is False
    assert [json.loads(line)["event"] for line in lines] == ["first"]
    assert cursor == len(complete)


def test_bridge_state_save_is_atomic(tmp_path):
    state_path = tmp_path / "bot-state.json"
    state = BridgeState(state_path)
    state.cursor = 123
    state.save()

    assert json.loads(state_path.read_text(encoding="utf-8"))["cursor"] == 123
    assert not state_path.with_name("bot-state.json.tmp").exists()


def test_disk_healing_does_not_depend_on_an_unrelated_stale_request(tmp_path):
    destination = tmp_path / "Album"
    destination.mkdir()
    (destination / "01 Song.flac").write_bytes(b"done")
    batch = DownloadBatch(
        channel_id=1,
        user_id=2,
        user_name="tester",
        query="Artist Album",
        total_files=1,
        request_ids=["request-1"],
        items={
            "request-1": DownloadItem(
                user="alice",
                path="music\\Artist\\Album\\01 Song.flac",
                dest=str(destination),
                label="01 Song.flac",
                status="error",
                error="stale transfer row",
            )
        },
    )

    healed = heal_downloads_present_on_disk({"batch-1": batch})

    assert healed == {"batch-1"}
    assert batch.items["request-1"].status == "finished"
    assert batch.items["request-1"].progress == 100
    assert batch.latest == "Finished 1/1"


def test_unix_json_call_round_trip(tmp_path):
    socket_path = tmp_path / "bridge.sock"
    ready = threading.Event()

    def serve():
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(socket_path))
            server.listen(1)
            ready.set()
            client, _ = server.accept()
            with client:
                while client.recv(65536):
                    pass
                client.sendall(b'{"ok":true,"message":"pong"}\n')

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    assert ready.wait(1)

    assert unix_json_call(socket_path, {"op": "ping"}, timeout=1) == {"ok": True, "message": "pong"}
    thread.join(timeout=1)


def test_unix_json_call_times_out_cleanly(tmp_path):
    socket_path = tmp_path / "bridge.sock"
    ready = threading.Event()

    def serve():
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(socket_path))
            server.listen(1)
            ready.set()
            client, _ = server.accept()
            with client:
                while client.recv(65536):
                    pass
                time.sleep(0.3)

    threading.Thread(target=serve, daemon=True).start()
    assert ready.wait(1)

    reply = unix_json_call(socket_path, {"op": "ping"}, timeout=0.1)

    assert reply == {"ok": False, "error": "bridge timed out after 0.1s"}