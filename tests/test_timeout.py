"""Timeout semantics, ported from failsafe-go's `timeout` package.

Sync mode is cooperative unless ``interrupt=True``, so the sync tests assert the
cooperative contract rather than Go's preemptive one; PORT.md item 1 records the
deviation. Async mode matches Go, via :func:`asyncio.timeout`.
"""

from __future__ import annotations

import asyncio
import threading

import pytest
from conftest import flaky

from failsafe import (
    AsyncFailsafe,
    Failsafe,
    Fallback,
    RetriesExceeded,
    RetryPolicy,
    Timeout,
    TimeoutExceeded,
    current_execution,
)
from failsafe._core import FakeClock


def test_requires_a_positive_limit() -> None:
    with pytest.raises(ValueError, match="positive"):
        Timeout(0)
    with pytest.raises(ValueError, match="positive"):
        Timeout(-1.0)


def test_fast_execution_passes_through(clock: FakeClock) -> None:
    assert Failsafe(Timeout(10.0), clock=clock).call(lambda: "ok") == "ok"


def test_sync_timeout_fires_between_attempts(clock: FakeClock) -> None:
    """Cooperative: the limit is enforced at instruction boundaries, not mid-call."""

    def slow() -> str:
        clock.advance(3.0)
        raise ConnectionError("slow")

    fs = Failsafe(Timeout(5.0), RetryPolicy(max_retries=9), clock=clock)
    with pytest.raises(TimeoutExceeded) as info:
        fs.call(slow)
    assert info.value.timeout == 5.0
    assert clock.now() >= 5.0


def test_timeout_is_retryable_by_an_outer_retry(clock: FakeClock) -> None:
    """TimeoutExceeded is an ordinary failure."""
    attempts = 0

    def slow() -> str:
        nonlocal attempts
        attempts += 1
        clock.advance(2.0)
        return "late"

    fs = Failsafe(RetryPolicy(max_retries=2), Timeout(1.0), clock=clock)
    with pytest.raises(RetriesExceeded) as info:
        fs.call(slow)
    assert isinstance(info.value.last_exception, TimeoutExceeded)
    assert attempts == 3, "each attempt gets its own timeout"


def test_timeout_outside_retry_bounds_all_attempts(clock: FakeClock) -> None:
    """Go: a timeout outside a retry cancels the inner retries."""
    attempts = 0

    def slow() -> str:
        nonlocal attempts
        attempts += 1
        clock.advance(2.0)
        raise ConnectionError("slow")

    fs = Failsafe(Timeout(5.0), RetryPolicy(max_retries=99), clock=clock)
    with pytest.raises(TimeoutExceeded):
        fs.call(slow)
    assert 2 <= attempts <= 4, attempts


def test_timeout_overwrites_an_inner_result(clock: FakeClock) -> None:
    """An inner fallback still processing must be interrupted."""

    def slow_fallback(ex: object) -> str:
        clock.advance(10.0)
        return "fallback result"

    fs = Failsafe(Timeout(5.0), Fallback(fn=slow_fallback), clock=clock)
    with pytest.raises(TimeoutExceeded):
        fs.call(flaky(9))


def test_on_timeout_listener_fires(clock: FakeClock) -> None:
    log: list[object] = []

    def slow() -> str:
        clock.advance(10.0)
        return "late"

    fs = Failsafe(Timeout(1.0, on_timeout=lambda e: log.append(e.exception)), clock=clock)
    with pytest.raises(TimeoutExceeded):
        fs.call(slow)
    assert isinstance(log[0], TimeoutExceeded)


def test_execution_exposes_the_deadline(clock: FakeClock) -> None:
    """Sync code sizes its own I/O timeouts from remaining()."""
    seen: list[float | None] = []

    def record() -> str:
        ex = current_execution()
        assert ex is not None
        seen.append(ex.remaining())
        return "ok"

    assert Failsafe(Timeout(5.0), clock=clock).call(record) == "ok"
    assert seen == [5.0]


def test_deadline_is_restored_after_the_race(clock: FakeClock) -> None:
    fs = Failsafe(Timeout(5.0), clock=clock)
    fs.call(lambda: "ok")
    assert current_execution() is None


def test_interrupting_timeout_frees_the_caller() -> None:
    """interrupt=True releases the caller on time; the thread runs on."""
    may_finish = threading.Event()
    finished = threading.Event()

    def slow() -> str:
        may_finish.wait(5.0)
        finished.set()
        return "late"

    fs = Failsafe(Timeout(0.05, interrupt=True))
    with pytest.raises(TimeoutExceeded):
        fs.call(slow)

    # The caller is back while the worker is still parked, which is the whole claim:
    # it was freed rather than waiting for the work to finish.
    assert not finished.is_set(), "the abandoned work is still running"

    may_finish.set()
    assert finished.wait(5.0), "...and leaks to completion, the documented cost"


def test_interrupting_timeout_returns_a_fast_result() -> None:
    fs = Failsafe(Timeout(5.0, interrupt=True))
    assert fs.call(lambda: "ok") == "ok"


# ----------------------------------------------------------------------- async


def test_async_timeout_really_interrupts() -> None:
    """Go's behaviour exactly: asyncio.timeout interrupts the awaiting attempt."""

    async def hang() -> str:
        await asyncio.sleep(30)
        return "never"

    async def scenario() -> None:
        started = asyncio.get_running_loop().time()
        with pytest.raises(TimeoutExceeded):
            await AsyncFailsafe(Timeout(0.05)).call(hang)
        assert asyncio.get_running_loop().time() - started < 1.0

    asyncio.run(scenario())


def test_async_timeout_abandons_inner_retries() -> None:
    attempts = 0

    async def slow() -> str:
        nonlocal attempts
        attempts += 1
        await asyncio.sleep(0.05)
        raise ConnectionError("slow")

    async def scenario() -> None:
        fs = AsyncFailsafe(Timeout(0.12), RetryPolicy(max_retries=99))
        with pytest.raises(TimeoutExceeded):
            await fs.call(slow)

    asyncio.run(scenario())
    assert 2 <= attempts <= 4, attempts


def test_async_timeout_is_retryable_by_an_outer_retry() -> None:
    attempts = 0

    async def slow() -> str:
        nonlocal attempts
        attempts += 1
        await asyncio.sleep(1.0)
        return "never"

    async def scenario() -> None:
        fs = AsyncFailsafe(RetryPolicy(max_retries=2), Timeout(0.02))
        with pytest.raises(RetriesExceeded) as info:
            await fs.call(slow)
        assert isinstance(info.value.last_exception, TimeoutExceeded)

    asyncio.run(scenario())
    assert attempts == 3


def test_async_cancellation_beats_the_timeout() -> None:
    """A cancelled task is terminal: no TimeoutExceeded, no retry."""

    async def hang() -> str:
        await asyncio.sleep(30)
        return "never"

    async def scenario() -> None:
        fs = AsyncFailsafe(RetryPolicy(max_retries=5), Timeout(10.0))
        task = asyncio.ensure_future(fs.call(hang))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())


def test_async_execution_exposes_the_deadline() -> None:
    seen: list[float | None] = []

    async def record() -> str:
        ex = current_execution()
        assert ex is not None
        seen.append(ex.remaining())
        return "ok"

    async def scenario() -> None:
        assert await AsyncFailsafe(Timeout(5.0)).call(record) == "ok"

    asyncio.run(scenario())
    assert seen[0] is not None
    assert 4.0 < seen[0] <= 5.0
