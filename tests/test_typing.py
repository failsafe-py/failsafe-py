"""Static typing guarantees, enforced by ``mypy --strict`` over this file.

There are no runtime assertions worth making here: the point is what the type checker
accepts and rejects. Each deliberate error carries a ``type: ignore[code]`` comment,
and strict mode's ``warn_unused_ignores`` fails the build if one stops being an error.
That makes this file a regression test for the checks themselves — delete the
:class:`~typing.ParamSpec` from ``call`` and the unused ignores light up.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from failsafe import AsyncFailsafe, CircuitBreaker, Failsafe, RetryPolicy


class Response:
    """A stand-in for a result type that is not interchangeable with ``int``."""

    code: int = 200


def send_request(url: str, *, timeout: float = 1.0) -> Response:
    return Response()


def count_rows(table: str) -> int:
    return 7


async def fetch(url: str) -> Response:
    return Response()


def test_the_result_type_comes_from_the_function() -> None:
    """One composition, two result types, no annotations."""
    failsafe = Failsafe(RetryPolicy(max_retries=1), CircuitBreaker())

    response: Response = failsafe.call(send_request, "https://example.test")
    rows: int = failsafe.call(count_rows, "accounts")

    assert response.code == 200
    assert rows == 7


def test_a_declared_result_type_is_checked() -> None:
    """Callers may state the type they expect; a mismatch is an error."""
    failsafe = Failsafe(RetryPolicy(max_retries=1))

    if TYPE_CHECKING:
        _wrong: int = failsafe.call(  # type: ignore[assignment]
            send_request, "https://example.test"
        )

    declared: Response = failsafe.call(send_request, "https://example.test")
    assert declared.code == 200


def test_arguments_are_checked_against_the_function() -> None:
    """The reason ``call`` takes a ParamSpec rather than ``...``.

    These calls are checked but never executed: each would raise at runtime, which
    is exactly what the type checker is here to prevent.
    """
    failsafe = Failsafe(RetryPolicy(max_retries=1))

    if TYPE_CHECKING:
        failsafe.call(send_request, 123)  # type: ignore[arg-type]
        failsafe.call(send_request)  # type: ignore[call-arg]
        failsafe.call(send_request, "https://x", retries=2)  # type: ignore[call-arg]

    assert failsafe.call(send_request, "https://example.test").code == 200


def test_the_decorator_preserves_the_signature() -> None:
    failsafe = Failsafe(RetryPolicy(max_retries=1))
    guarded = failsafe(send_request)

    if TYPE_CHECKING:
        guarded(123)  # type: ignore[arg-type]

    response: Response = guarded("https://example.test")
    assert response.code == 200


def test_async_calls_are_checked_too() -> None:
    async def main() -> None:
        failsafe = AsyncFailsafe(RetryPolicy(max_retries=1))

        if TYPE_CHECKING:
            await failsafe.call(fetch, 123)  # type: ignore[arg-type]

        response: Response = await failsafe.call(fetch, "https://example.test")
        assert response.code == 200

    asyncio.run(main())
