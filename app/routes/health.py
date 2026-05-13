from __future__ import annotations

import json
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from socketserver import TCPServer
from threading import Thread
from typing import Any, Callable
from urllib.parse import urlparse

from app.logging_config import current_log_level_name, write_runtime_log_level

logger = logging.getLogger(__name__)

HealthProvider = Callable[[], dict[str, Any]]


class _HealthThreadingHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False

    def server_bind(self) -> None:
        TCPServer.server_bind(self)
        host, port = self.server_address[:2]
        self.server_name = str(host)
        self.server_port = int(port)


class SupervisorHealthHttpServer:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        health_provider: HealthProvider,
        runtime_log_level_path: str | Path = ".runtime/log-level",
    ) -> None:
        self.host = host
        self.port = port
        self.health_provider = health_provider
        self.runtime_log_level_path = Path(runtime_log_level_path)
        self._server: ThreadingHTTPServer | None = None
        self._thread: Thread | None = None

    @property
    def bound_port(self) -> int | None:
        if self._server is None:
            return None
        return int(self._server.server_address[1])

    def start(self) -> None:
        if self._server is not None:
            return

        provider = self.health_provider
        runtime_log_level_path = self.runtime_log_level_path

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
                document = provider()
                path = urlparse(self.path).path
                if path == "/health":
                    _write_json(self, 200, document)
                elif path == "/health/summary":
                    _write_json(self, 200, document.get("summary", {}))
                elif path == "/health/cameras":
                    _write_json(self, 200, document.get("cameras", []))
                elif path == "/admin/log-level":
                    _write_json(
                        self,
                        200,
                        {"level": current_log_level_name(), "runtime_path": str(runtime_log_level_path)},
                    )
                else:
                    _write_json(self, 404, {"error": "not_found"})

            def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
                path = urlparse(self.path).path
                if path != "/admin/log-level":
                    _write_json(self, 404, {"error": "not_found"})
                    return
                try:
                    length = int(self.headers.get("Content-Length", "0"))
                except ValueError:
                    length = 0
                try:
                    payload = json.loads(self.rfile.read(length).decode("utf-8") if length > 0 else "{}")
                    level = write_runtime_log_level(runtime_log_level_path, str(payload.get("level", "")), source="admin_http")
                except (json.JSONDecodeError, ValueError) as exc:
                    _write_json(self, 422, {"error": "invalid_log_level", "message": str(exc)})
                    return
                _write_json(self, 200, {"level": level, "runtime_path": str(runtime_log_level_path)})

            def log_message(self, format: str, *args) -> None:  # noqa: A002 - stdlib signature
                logger.debug("health_http_request " + format, *args)

        self._server = _HealthThreadingHTTPServer((self.host, self.port), Handler)
        self._thread = Thread(target=self._server.serve_forever, name="active-camera-health-http", daemon=True)
        self._thread.start()
        logger.info("active_camera_health_server_started host=%s port=%s", self.host, self.bound_port)

    def stop(self) -> None:
        server = self._server
        thread = self._thread
        self._server = None
        self._thread = None
        if server is None:
            return
        server.shutdown()
        server.server_close()
        if thread is not None:
            thread.join(timeout=2)
        logger.info("active_camera_health_server_stopped")


def _write_json(handler: BaseHTTPRequestHandler, status_code: int, payload: Any) -> None:
    body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    handler.send_response(status_code)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)
