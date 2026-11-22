"""Retry a flaky HTTP endpoint, honouring ``Retry-After``.

The server answers 429 twice with a ``Retry-After: 1`` header, then 200. The transport
retries both times, waiting as the server asked rather than on its own schedule.

    uv run --extra httpx examples/httpx_retry.py
"""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import httpx

from failsafe import ScheduledRetryEvent
from failsafe.httpx import FailsafeTransport, retry_policy

FAILURES_BEFORE_SUCCESS = 2


class FlakyHandler(BaseHTTPRequestHandler):
    """Answers 429 with a Retry-After for the first few requests, then 200."""

    requests = 0

    def do_GET(self) -> None:
        type(self).requests += 1
        if type(self).requests <= FAILURES_BEFORE_SUCCESS:
            self.send_response(429)
            self.send_header("Retry-After", "1")
            self.end_headers()
            self.wfile.write(b"slow down")
            return
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"pong")

    def log_message(self, format: str, *args: object) -> None:
        """Silence the default stderr logging."""


def main() -> None:
    server = HTTPServer(("127.0.0.1", 0), FlakyHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_port}/"

    def on_retry_scheduled(event: ScheduledRetryEvent) -> None:
        print(f"  retry {event.execution.attempts} after {event.delay:.1f}s")

    # retry_policy() already knows which HTTP failures are worth retrying and reads
    # Retry-After off the response.
    transport = FailsafeTransport(retry_policy(on_retry_scheduled=on_retry_scheduled))

    print(f"GET {url}")
    with httpx.Client(transport=transport) as client:
        response = client.get(url)

    print(f"  {response.status_code} {response.text}")
    print(f"  took {FlakyHandler.requests} requests")
    server.shutdown()


if __name__ == "__main__":
    main()
