"""failsafe.httpx transports, ported from failsafe-go's failsafehttp client corpus.

Every test runs against an in-process transport. Nothing here touches a network.

The inner transport is :class:`Recorder` wherever a request body matters, because
:class:`httpx.MockTransport` reads the request before the handler sees it.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Callable
from typing import Any

import httpx
import pytest

from failsafe import (
    Bulkhead,
    CircuitBreaker,
    RetryPolicy,
    Timeout,
)
from failsafe._core import FakeClock
from failsafe.httpx import (
    AsyncFailsafeTransport,
    FailsafeTransport,
    is_retryable,
    retry_after_delay,
    retry_policy,
)

Handler = Callable[[httpx.Request], httpx.Response]


class Recorder(httpx.BaseTransport, httpx.AsyncBaseTransport):
    """An inner transport that records the bytes of every attempt it is handed.

    Deliberately not :class:`httpx.MockTransport`: that calls ``request.read()``
    before the handler, which buffers a streaming body back onto the request and so
    makes a broken rewind invisible. Draining the stream is what a real transport
    does, and it is the only way these tests can witness a body sent twice.
    """

    def __init__(self, *statuses: int, body: str = "ok") -> None:
        self.sent: list[bytes] = []
        self._remaining = list(statuses)
        self._body = body

    def _respond(self, request: httpx.Request) -> httpx.Response:
        self.sent.append(b"".join(request.stream))  # type: ignore[arg-type]
        status = self._remaining.pop(0) if self._remaining else 200
        return httpx.Response(status, text=self._body)

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        return self._respond(request)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.sent.append(b"".join([chunk async for chunk in request.stream]))  # type: ignore[union-attr]
        status = self._remaining.pop(0) if self._remaining else 200
        return httpx.Response(status, text=self._body)


def responder(*statuses: int, body: str = "ok") -> tuple[Recorder, list[bytes]]:
    """A recording inner transport and the list of bodies it has received."""
    recorder = Recorder(*statuses, body=body)
    return recorder, recorder.sent


def client(*policies: Any, handler: httpx.BaseTransport) -> httpx.Client:
    """A client whose transport applies ``policies`` over a recording transport."""
    return httpx.Client(transport=FailsafeTransport(*policies, transport=handler))


def test_successful_request_passes_through() -> None:
    """A policy stack that never fires must not disturb the response."""
    handler, sent = responder(200, body="hello")
    with client(retry_policy(max_retries=2, delay=None), handler=handler) as http:
        response = http.get("http://example.test/")

    assert response.status_code == 200
    assert response.text == "hello"
    assert len(sent) == 1


def test_retries_a_server_error_then_succeeds() -> None:
    """Go TestRetryPolicyWith500: 5xx is retryable."""
    handler, sent = responder(500, 500, 200)
    with client(retry_policy(max_retries=3, delay=None), handler=handler) as http:
        response = http.get("http://example.test/")

    assert response.status_code == 200
    assert len(sent) == 3


def test_does_not_retry_a_client_error() -> None:
    """A deliberate 400 is the server's answer, not a transient failure."""
    handler, sent = responder(400)
    with client(retry_policy(max_retries=3, delay=None), handler=handler) as http:
        response = http.get("http://example.test/")

    assert response.status_code == 400
    assert len(sent) == 1, "a 400 must not be retried"


def test_retries_a_connection_error() -> None:
    """Go retries url errors; httpx models them as TransportError."""
    attempts = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise httpx.ConnectError("refused", request=request)
        return httpx.Response(200, text="ok")

    with client(
        retry_policy(max_retries=3, delay=None), handler=httpx.MockTransport(handle)
    ) as http:
        assert http.get("http://example.test/").status_code == 200
    assert attempts == 3


def test_does_not_retry_an_unsupported_scheme() -> None:
    """Go explicitly excludes this: a retry cannot invent protocol support."""

    def handle(request: httpx.Request) -> httpx.Response:
        raise httpx.UnsupportedProtocol("unsupported", request=request)

    with (
        client(
            retry_policy(max_retries=3, delay=None), handler=httpx.MockTransport(handle)
        ) as http,
        pytest.raises(httpx.UnsupportedProtocol),
    ):
        http.get("http://example.test/")


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (200, False),
        (400, False),
        (404, False),
        (408, False),
        (429, True),
        (500, True),
        (501, False),
        (503, True),
    ],
)
def test_retryable_status_codes(status: int, expected: bool) -> None:
    """The documented retryable set, including Go's 501 exclusion."""
    assert is_retryable(httpx.Response(status), None) is expected


def test_streaming_request_body_is_resent_on_retry() -> None:
    """The failure mode this guards: a retry silently sending an empty body."""
    handler, sent = responder(503, 200)
    with client(retry_policy(max_retries=2, delay=None), handler=handler) as http:
        response = http.post("http://example.test/", content=iter([b"part1", b"part2"]))

    assert response.status_code == 200
    assert len(sent) == 2
    assert sent == [b"part1part2", b"part1part2"]


def test_bytes_request_body_is_resent_on_retry() -> None:
    """The easy case, which must keep working alongside the streaming one."""
    handler, sent = responder(503, 200)
    with client(retry_policy(max_retries=2, delay=None), handler=handler) as http:
        response = http.post("http://example.test/", content=b"payload")

    assert response.status_code == 200
    assert sent == [b"payload", b"payload"]


def test_request_without_a_body_is_retried() -> None:
    """A GET has no body to rewind; the rewind path must not break it."""
    handler, sent = responder(503, 200)
    with client(retry_policy(max_retries=2, delay=None), handler=handler) as http:
        assert http.get("http://example.test/").status_code == 200
    assert sent == [b"", b""]


def test_response_stream_survives_the_transport() -> None:
    """A streamed response must still be streamable through the policy stack."""
    chunks = [b"alpha", b"beta", b"gamma"]

    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=httpx.ByteStream(b"".join(chunks)))

    with (
        client(
            retry_policy(max_retries=2, delay=None), handler=httpx.MockTransport(handle)
        ) as http,
        http.stream("GET", "http://example.test/") as response,
    ):
        assert response.status_code == 200
        received = list(response.iter_bytes())

    assert b"".join(received) == b"alphabetagamma"


def test_retry_after_seconds_is_honoured(clock: FakeClock) -> None:
    """Go's DelayFunc: a Retry-After of N seconds delays the retry by N seconds."""

    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"Retry-After": "7"})

    transport = FailsafeTransport(
        RetryPolicy(
            handle=is_retryable,
            delay=retry_after_delay,
            max_retries=1,
            return_last_failure=True,
        ),
        transport=httpx.MockTransport(handle),
        clock=clock,
    )

    with httpx.Client(transport=transport) as http:
        assert http.get("http://example.test/").status_code == 429
    assert clock.slept == [7.0]


def test_retry_after_is_ignored_for_other_statuses() -> None:
    """Go consults Retry-After only on 429 and 503."""
    response = httpx.Response(500, headers={"Retry-After": "7"})
    execution = _execution_with(response)
    assert retry_after_delay(execution) == 0.0


def test_retry_after_http_date_is_converted_to_a_delay() -> None:
    """The header's other legal form, which Go's integer parse silently ignores."""
    import datetime

    when = datetime.datetime.now(tz=datetime.UTC) + datetime.timedelta(seconds=30)
    stamp = when.strftime("%a, %d %b %Y %H:%M:%S GMT")
    response = httpx.Response(503, headers={"Retry-After": stamp})

    delay = retry_after_delay(_execution_with(response))
    assert 25 <= delay <= 31, delay


def test_retry_after_garbage_falls_back_to_no_delay() -> None:
    """A malformed header must not raise into the retry path."""
    response = httpx.Response(429, headers={"Retry-After": "soon please"})
    assert retry_after_delay(_execution_with(response)) == 0.0


def test_retry_after_in_the_past_never_yields_a_negative_delay() -> None:
    """A negative delay would become a negative sleep in the driver."""
    import datetime

    past = datetime.datetime.now(tz=datetime.UTC) - datetime.timedelta(hours=5)
    stamp = past.strftime("%a, %d %b %Y %H:%M:%S GMT")
    assert (
        retry_after_delay(_execution_with(httpx.Response(429, headers={"Retry-After": stamp})))
        == 0.0
    )
    assert (
        retry_after_delay(_execution_with(httpx.Response(429, headers={"Retry-After": "-5"})))
        == 0.0
    )


def test_retry_after_without_a_response_is_zero() -> None:
    """The delay is also used by breakers, which may have no last result."""
    assert retry_after_delay(_execution_with(None)) == 0.0


def _execution_with(response: httpx.Response | None):  # type: ignore[no-untyped-def]
    """An Execution whose last result is ``response``."""
    from failsafe._core import Execution

    execution = Execution(FakeClock())
    execution.last_result = response
    return execution


def test_circuit_breaker_opens_and_rejects_without_a_request() -> None:
    """A rejected execution never reaches the transport."""
    handler, sent = responder(500, 500, 500, 200)
    breaker = CircuitBreaker(failure_threshold=2, handle=None, delay=60.0)
    breaker.open()

    transport = FailsafeTransport(breaker, transport=handler)
    with (
        httpx.Client(transport=transport) as http,
        pytest.raises(httpx.TransportError, match="circuit breaker open"),
    ):
        http.get("http://example.test/")

    assert sent == [], "the breaker rejected before any request was made"


def test_policy_rejection_is_raised_as_a_transport_error() -> None:
    """httpx callers catch httpx.HTTPError; a bare FailsafeError would escape that."""
    bulkhead = Bulkhead(1)
    bulkhead.try_acquire_permit()
    handler, sent = responder(200)

    transport = FailsafeTransport(bulkhead, transport=handler)
    with httpx.Client(transport=transport) as http, pytest.raises(httpx.HTTPError) as caught:
        http.get("http://example.test/")

    assert "bulkhead full" in str(caught.value)
    assert sent == []


def test_async_transport_retries_and_rewinds() -> None:
    """The async transport must rewind streaming bodies exactly as the sync one does."""
    handler, sent = responder(503, 200)

    async def scenario() -> httpx.Response:
        transport = AsyncFailsafeTransport(
            retry_policy(max_retries=2, delay=None),
            transport=handler,
        )
        async with httpx.AsyncClient(transport=transport) as http:

            async def chunks() -> AsyncIterator[bytes]:
                yield b"part1"
                yield b"part2"

            return await http.post("http://example.test/", content=chunks())

    response = asyncio.run(scenario())
    assert response.status_code == 200
    assert sent == [b"part1part2", b"part1part2"]


def test_sync_and_async_transports_share_policy_state() -> None:
    """One breaker, two transports, one circuit."""
    breaker = CircuitBreaker(failure_threshold=1, handle=is_retryable, delay=60.0)
    handler, sent = responder(500, 200)

    sync_transport = FailsafeTransport(breaker, transport=handler)
    with httpx.Client(transport=sync_transport) as http:
        assert http.get("http://example.test/").status_code == 500
    assert breaker.state == "open"

    async def scenario() -> None:
        transport = AsyncFailsafeTransport(breaker, transport=handler)
        async with httpx.AsyncClient(transport=transport) as http:
            with pytest.raises(httpx.TransportError):
                await http.get("http://example.test/")

    asyncio.run(scenario())
    assert len(sent) == 1, "the async call was rejected by the breaker the sync call opened"


def test_retry_policy_defaults_are_overridable() -> None:
    """retry_policy is a convenience, not a cage."""
    handler, sent = responder(500, 500, 500, 200)
    with client(retry_policy(max_retries=1, delay=None), handler=handler) as http:
        assert http.get("http://example.test/").status_code == 500
    assert len(sent) == 2, "max_retries=1 means two attempts"


def test_the_policy_deadline_reaches_httpx() -> None:
    """A sync Timeout bounds the round trip, not just what gets reported.

    httpx cannot be interrupted mid-request, so the deadline only becomes real if the
    transport hands it over. It goes into ``request.extensions["timeout"]``, which is
    this transport's equivalent of the request context failsafe-go threads per attempt.
    """
    seen: list[dict[str, float]] = []

    class Recording(httpx.BaseTransport):
        def handle_request(self, request: httpx.Request) -> httpx.Response:
            seen.append(request.extensions["timeout"])
            return httpx.Response(200)

    transport = FailsafeTransport(Timeout(2.0), transport=Recording())
    httpx.Client(transport=transport, timeout=30.0).get("http://x/")

    assert seen, "the inner transport ran"
    assert all(value == pytest.approx(2.0, abs=0.1) for value in seen[0].values()), (
        f"every phase narrowed to the policy budget, got {seen[0]}"
    )


def test_a_stricter_client_timeout_is_not_widened() -> None:
    """The budget narrows each phase; it never relaxes what the caller asked for."""
    seen: list[dict[str, float]] = []

    class Recording(httpx.BaseTransport):
        def handle_request(self, request: httpx.Request) -> httpx.Response:
            seen.append(request.extensions["timeout"])
            return httpx.Response(200)

    transport = FailsafeTransport(Timeout(30.0), transport=Recording())
    httpx.Client(transport=transport, timeout=0.5).get("http://x/")

    assert seen[0]["read"] == pytest.approx(0.5), f"client's stricter value kept, got {seen[0]}"


def test_the_async_timeout_interrupts_a_hung_request() -> None:
    """Async needs no deadline handover: asyncio.timeout cancels the attempt outright.

    The sync transport narrows ``request.extensions["timeout"]`` because a blocking
    call cannot be interrupted. Async can be, so a backend that never replies is cut
    off at the policy's budget with nothing handed to httpx.
    """

    class Hanging(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            await asyncio.sleep(30)
            raise AssertionError("the attempt should have been cancelled")

    async def main() -> float:
        transport = AsyncFailsafeTransport(Timeout(0.05), transport=Hanging())
        client = httpx.AsyncClient(transport=transport, timeout=None)
        start = time.perf_counter()
        with pytest.raises(httpx.TransportError):
            await client.get("http://x/")
        return time.perf_counter() - start

    elapsed = asyncio.run(main())
    assert elapsed < 1.0, f"cancelled at the deadline, not after the sleep, got {elapsed:.2f}s"
