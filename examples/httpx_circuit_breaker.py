"""Stop calling a failing backend, then let it back in once it recovers.

The server fails the first few requests. After two failures the breaker opens and
rejects calls without touching the network; once the delay elapses it admits a trial,
and the recovered backend closes it again.

    uv run --extra httpx examples/httpx_circuit_breaker.py
"""

from __future__ import annotations

import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import httpx

from failsafe import CircuitBreaker, StateChangedEvent
from failsafe.httpx import FailsafeTransport, is_retryable

FAILURES_BEFORE_RECOVERY = 2
OPEN_FOR_SECONDS = 1.0


class RecoveringHandler(BaseHTTPRequestHandler):
    """Fails the first few requests, then serves normally."""

    requests = 0

    def do_GET(self) -> None:
        type(self).requests += 1
        if type(self).requests <= FAILURES_BEFORE_RECOVERY:
            self.send_response(503)
            self.end_headers()
            self.wfile.write(b"unavailable")
            return
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"pong")

    def log_message(self, format: str, *args: object) -> None:
        """Silence the default stderr logging."""


def main() -> None:
    server = HTTPServer(("127.0.0.1", 0), RecoveringHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_port}/"

    def on_state_change(event: StateChangedEvent) -> None:
        print(f"  breaker {event.previous_state} -> {event.state}")

    # handle= decides what counts as a failure; is_retryable treats 5xx as one.
    breaker = CircuitBreaker(
        failure_threshold=FAILURES_BEFORE_RECOVERY,
        delay=OPEN_FOR_SECONDS,
        handle=is_retryable,
        on_state_change=on_state_change,
    )

    with httpx.Client(transport=FailsafeTransport(breaker)) as client:
        for i in range(1, 6):
            try:
                response = client.get(url)
                print(f"request {i}: {response.status_code}")
            except httpx.TransportError as exc:
                print(f"request {i}: rejected without calling the backend ({exc})")
            if i == 3:
                print(f"  waiting {OPEN_FOR_SECONDS}s for the breaker to admit a trial")
                time.sleep(OPEN_FOR_SECONDS + 0.1)

    print(f"\nthe backend saw {RecoveringHandler.requests} requests, not 5")
    server.shutdown()


if __name__ == "__main__":
    main()
