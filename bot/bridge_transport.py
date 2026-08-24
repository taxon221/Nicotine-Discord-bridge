from __future__ import annotations

import json
import socket
from pathlib import Path
from typing import Any


DEFAULT_TIMEOUT_SECONDS = 8.0
MAX_RESPONSE_BYTES = 4 * 1024 * 1024


def unix_json_call(
    socket_path: Path,
    payload: dict[str, Any],
    *,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Call the local bridge without allowing a dead socket to hang the bot."""
    path = Path(socket_path)
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.settimeout(max(0.1, float(timeout)))
            sock.connect(str(path))
            sock.sendall((json.dumps(payload) + "\n").encode("utf-8"))
            sock.shutdown(socket.SHUT_WR)
            chunks: list[bytes] = []
            received = 0
            while True:
                data = sock.recv(65536)
                if not data:
                    break
                received += len(data)
                if received > MAX_RESPONSE_BYTES:
                    return {"ok": False, "error": "bridge response exceeded 4 MiB"}
                chunks.append(data)
    except (TimeoutError, socket.timeout):
        return {"ok": False, "error": f"bridge timed out after {float(timeout):g}s"}
    except FileNotFoundError:
        return {"ok": False, "error": f"bridge socket not found: {path}"}
    except ConnectionRefusedError:
        return {"ok": False, "error": "bridge socket exists but Nicotine is not accepting connections"}
    except PermissionError:
        return {"ok": False, "error": f"permission denied opening bridge socket: {path}"}
    except OSError as exc:
        return {"ok": False, "error": f"bridge connection failed: {exc}"}

    raw = b"".join(chunks).decode("utf-8", errors="replace").strip()
    if not raw:
        return {"ok": False, "error": "empty response from bridge"}
    try:
        response = json.loads(raw)
    except json.JSONDecodeError as exc:
        return {"ok": False, "error": f"invalid JSON response from bridge: {exc.msg}"}
    if not isinstance(response, dict):
        return {"ok": False, "error": "bridge response was not a JSON object"}
    return response
