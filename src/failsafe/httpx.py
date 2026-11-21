"""Failsafe transports for httpx, the analogue of failsafe-go's ``failsafehttp``.

Install with the ``httpx`` extra::

    pip install 'failsafe-py[httpx]'

A transport wraps another transport, so policies see each HTTP round trip as one
execution::

    transport = FailsafeTransport(retry_policy(), circuit_breaker)
    client = httpx.Client(transport=transport)

Only the client side is ported. Go's server-side handlers have no analogue here: an
ASGI middleware is a different shape, and nothing in the core needs one.
"""

from __future__ import annotations

import datetime
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING, Any

import httpx

from ._core import SYSTEM_CLOCK, Clock, Execution
from ._exceptions import FailsafeError
from .retry import RetryPolicy

if TYPE_CHECKING:
    from ._core import Policy

__all__ = [
    "AsyncFailsafeTransport",
    "FailsafeTransport",
    "is_retryable",
    "retry_after_delay",
    "retry_policy",
]

RETRYABLE_STATUS_CODES = frozenset({408, 429})
"""Non-5xx statuses worth retrying.

``408 Request Timeout`` is the server reporting it gave up waiting for the request,
and ``429 Too Many Requests`` invites a later attempt. Go retries only the 429; see
PORT.md item 18 for why 408 is included here.
"""

_NOT_RETRYABLE_SERVER_ERRORS = frozenset({501})
"""``501 Not Implemented`` will not become implemented on a retry, as Go excludes it."""

_RETRY_AFTER_STATUS_CODES = frozenset({429, 503})
"""Go honours ``Retry-After`` only on these two, ignoring it elsewhere."""


def is_retryable(response: httpx.Response | None, exc: Exception | None) -> bool:
    """Whether a round trip is worth retrying, as Go's ``NewRetryPolicyBuilder`` judges.

    Retryable: any :exc:`httpx.TransportError` (connection failures, timeouts, read
    errors), plus responses with status 408, 429, or 5xx other than 501.

    Not retryable: a response the server answered deliberately (4xx other than 408 and
    429), an unsupported scheme, an invalid URL, or too many redirects — none of which
    a second identical request would fix.

    Args:
        response: The response, if one arrived.
        exc: The exception raised, if the round trip failed.

    Returns:
        Whether to retry.
    """
    if exc is not None:
        if isinstance(exc, httpx.UnsupportedProtocol | httpx.InvalidURL | httpx.TooManyRedirects):
            return False
        return isinstance(exc, httpx.TransportError)
    if response is None:
        return False
    status = response.status_code
    if status in RETRYABLE_STATUS_CODES:
        return True
    return status >= 500 and status not in _NOT_RETRYABLE_SERVER_ERRORS


def retry_after_delay(ex: Execution | None) -> float:
    """Delay according to a response's ``Retry-After`` header.

    Usable anywhere a callable delay is accepted, in a
    :class:`~failsafe.RetryPolicy` or a :class:`~failsafe.CircuitBreaker`, mirroring
    Go's ``DelayFunc``::

        CircuitBreaker(handle=Result(429), delay=retry_after_delay)

    Both header forms are honoured: delay-seconds, and an HTTP-date, which is
    converted to a delay from now. Only ``429`` and ``503`` responses are consulted,
    as in Go.

    Args:
        ex: The execution whose last result is inspected, or ``None`` for a manual
            circuit breaker transition, which has no result to read.

    Returns:
        Seconds to wait, or ``0.0`` when the header is absent or unusable, letting the
        policy's own delay apply.
    """
    if ex is None:
        return 0.0
    response = ex.last_result
    if not isinstance(response, httpx.Response):
        return 0.0
    if response.status_code not in _RETRY_AFTER_STATUS_CODES:
        return 0.0
    header = response.headers.get("retry-after")
    if header is None:
        return 0.0
    try:
        return max(0.0, float(int(header)))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(header)
    except (TypeError, ValueError):
        return 0.0
    now = datetime.datetime.now(tz=when.tzinfo)
    return max(0.0, (when - now).total_seconds())


def retry_policy(**kwargs: Any) -> RetryPolicy:
    """Build a :class:`~failsafe.RetryPolicy` that knows which HTTP failures to retry.

    Defaults to :func:`is_retryable` for ``handle`` and :func:`retry_after_delay` for
    ``delay``; both are overridable, as are all other
    :class:`~failsafe.RetryPolicy` arguments::

        retry_policy(max_retries=3, delay=Backoff(1.0, 30.0))

    ``return_last_failure`` defaults to ``True``, so exhausting the retries on a
    failing *response* yields that response rather than
    :exc:`~failsafe.RetriesExceeded`: a 500 is still an answer the caller may want to
    inspect, and Go returns it too.

    Args:
        **kwargs: Passed to :class:`~failsafe.RetryPolicy`.

    Returns:
        A retry policy suited to HTTP.
    """
    kwargs.setdefault("handle", is_retryable)
    kwargs.setdefault("delay", retry_after_delay)
    kwargs.setdefault("return_last_failure", True)
    return RetryPolicy(**kwargs)


def _buffer_body(request: httpx.Request) -> None:
    """Make a streaming request body replayable, so a retry resends it.

    ``Request.read()`` drains the stream and swaps in a :class:`httpx.ByteStream` over
    the buffered bytes, which every later attempt replays. Without this a retry sends
    an empty body: the first attempt consumed the iterator.
    """
    if not isinstance(request.stream, httpx.ByteStream):
        request.read()


class FailsafeTransport(httpx.BaseTransport):
    """An :class:`httpx.BaseTransport` that applies policies to each round trip.

    Policies are composed outermost-first, exactly as in
    :class:`~failsafe.Failsafe`::

        transport = FailsafeTransport(retry_policy(), breaker)
        client = httpx.Client(transport=transport)

    A streaming request body is buffered once so a retry can resend it; a request
    whose body httpx already holds in memory is left alone.

    Policy rejections that never reached the network (an open breaker, a full
    bulkhead) surface as :exc:`httpx.TransportError`, so callers catch one family.

    Responses are returned with their stream intact, so
    :meth:`httpx.Client.stream` works through the transport. Note that policies judge
    the *response headers*: a stream that fails midway has already been judged a
    success, matching Go, where the policy result is recorded before the body is read.

    Args:
        *policies: The policies to apply, outermost first.
        transport: The transport to wrap. Defaults to :class:`httpx.HTTPTransport`.
        clock: The time source policies delay against. Injectable so tests need not
            sleep.
    """

    def __init__(
        self,
        *policies: Policy,
        transport: httpx.BaseTransport | None = None,
        clock: Clock = SYSTEM_CLOCK,
    ) -> None:
        from ._sync import Failsafe

        self._next = transport if transport is not None else httpx.HTTPTransport()
        self._failsafe = Failsafe(*policies, clock=clock)

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        """Perform one failsafe round trip."""
        _buffer_body(request)

        def attempt() -> httpx.Response:
            return self._next.handle_request(request)

        try:
            return self._failsafe.call(attempt)
        except FailsafeError as exc:
            raise _as_http_error(exc, request) from exc

    def close(self) -> None:
        """Close the wrapped transport."""
        self._next.close()


class AsyncFailsafeTransport(httpx.AsyncBaseTransport):
    """The async counterpart of :class:`FailsafeTransport`.

    Shares policy state with any :class:`FailsafeTransport` built from the same policy
    objects, so one circuit breaker can guard both.

    Args:
        *policies: The policies to apply, outermost first.
        transport: The transport to wrap. Defaults to :class:`httpx.AsyncHTTPTransport`.
        clock: The time source policies delay against. Injectable so tests need not
            sleep.
    """

    def __init__(
        self,
        *policies: Policy,
        transport: httpx.AsyncBaseTransport | None = None,
        clock: Clock = SYSTEM_CLOCK,
    ) -> None:
        from ._async import AsyncFailsafe

        self._next = transport if transport is not None else httpx.AsyncHTTPTransport()
        self._failsafe = AsyncFailsafe(*policies, clock=clock)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        """Perform one failsafe round trip."""
        await _abuffer_body(request)

        async def attempt() -> httpx.Response:
            return await self._next.handle_async_request(request)

        try:
            return await self._failsafe.call(attempt)
        except FailsafeError as exc:
            raise _as_http_error(exc, request) from exc

    async def aclose(self) -> None:
        """Close the wrapped transport."""
        await self._next.aclose()


async def _abuffer_body(request: httpx.Request) -> None:
    """The async counterpart of :func:`_buffer_body`."""
    if not isinstance(request.stream, httpx.ByteStream):
        await request.aread()


def _as_http_error(exc: FailsafeError, request: httpx.Request) -> Exception:
    """Surface a policy rejection as an httpx error, so client code catches one family.

    A policy that never ran the request (an open breaker, a full bulkhead) has no
    response to return, and httpx callers expect :exc:`httpx.HTTPError`.
    """
    return httpx.TransportError(str(exc), request=request)
