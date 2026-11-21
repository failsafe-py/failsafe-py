"""Bulkhead semantics, ported from failsafe-go's bulkhead corpus."""

from __future__ import annotations

import asyncio
import threading
import time

import pytest

from failsafe import AsyncFailsafe, Bulkhead, BulkheadFull, Failsafe, RetryPolicy
from failsafe._core import FakeClock


def test_try_acquire_permit_and_release_permit() -> None:
    """Go TestTryAcquirePermitAndReleasePermit."""
    bulkhead = Bulkhead(2)

    assert bulkhead.try_acquire_permit()
    assert bulkhead.try_acquire_permit()
    assert not bulkhead.try_acquire_permit()

    bulkhead.release_permit()
    assert bulkhead.try_acquire_permit()
    assert not bulkhead.try_acquire_permit()

    bulkhead.release_permit()
    bulkhead.release_permit()
    assert bulkhead.try_acquire_permit()
    assert bulkhead.try_acquire_permit()
    assert not bulkhead.try_acquire_permit()


def test_acquire_permit_waits_for_a_release() -> None:
    """Go TestAcquirePermit: the third acquire blocks until a permit comes back."""
    bulkhead = Bulkhead(2)
    bulkhead.try_acquire_permit()
    bulkhead.try_acquire_permit()

    def release_later() -> None:
        time.sleep(0.05)
        bulkhead.release_permit()

    releaser = threading.Thread(target=release_later)
    releaser.start()
    started = time.monotonic()
    bulkhead.acquire_permit()
    elapsed = time.monotonic() - started
    releaser.join()

    assert elapsed >= 0.05
    assert bulkhead.used == 2


def test_acquire_permit_with_timeout_raises_when_full() -> None:
    """Go TestAcquirePermitWithMaxWaitTime."""
    bulkhead = Bulkhead(1)

    bulkhead.acquire_permit(timeout=0.05)
    with pytest.raises(BulkheadFull, match="max concurrency: 1"):
        bulkhead.acquire_permit(timeout=0.05)


def test_release_without_acquire_is_an_error() -> None:
    """An unbalanced release is a bug in the caller, not a silent no-op."""
    with pytest.raises(RuntimeError, match="without a matching acquire"):
        Bulkhead(1).release_permit()


def test_executor_rejects_when_full(clock: FakeClock) -> None:
    """With the default zero wait, an execution with no free permit fails at once."""
    events: list[str] = []
    bulkhead = Bulkhead(1, on_full=lambda _e: events.append("full"))
    bulkhead.try_acquire_permit()

    with pytest.raises(BulkheadFull):
        Failsafe(bulkhead, clock=clock).call(lambda: "ok")
    assert events == ["full"]


def test_executor_releases_the_permit_after_an_execution() -> None:
    """The permit must come back whether the execution succeeds or raises."""
    bulkhead = Bulkhead(1)
    fs = Failsafe(bulkhead)

    assert fs.call(lambda: "ok") == "ok"
    assert bulkhead.used == 0

    def boom() -> str:
        raise ConnectionError("boom")

    with pytest.raises(ConnectionError):
        fs.call(boom)
    assert bulkhead.used == 0, "a failing execution still releases its permit"


def test_retry_reacquires_a_permit_per_attempt() -> None:
    """PLAN section 8 item 7: the permit is held per attempt, not per execution.

    A permit held across retry delays would deadlock a bulkhead of one against its
    own retries; releasing per attempt lets other callers interleave.
    """
    bulkhead = Bulkhead(1)
    held: list[int] = []
    attempts = 0

    def flaky() -> str:
        nonlocal attempts
        attempts += 1
        held.append(bulkhead.used)
        if attempts < 3:
            raise ConnectionError("boom")
        return "ok"

    fs = Failsafe(RetryPolicy(max_retries=3, delay=None), bulkhead)
    assert fs.call(flaky) == "ok"

    assert attempts == 3
    assert held == [1, 1, 1], "each attempt held exactly one permit"
    assert bulkhead.used == 0


def test_permit_is_released_when_an_interrupted_attempt_finishes() -> None:
    """An interrupting timeout frees the caller, not the worker thread.

    Python cannot kill the abandoned thread, so the permit stays held until the slow
    call returns on its own; the generator's ``finally`` then releases it.
    """
    from failsafe import Timeout, TimeoutExceeded

    bulkhead = Bulkhead(1)

    def slow() -> str:
        time.sleep(0.2)
        return "late"

    with pytest.raises(TimeoutExceeded):
        Failsafe(Timeout(0.02, interrupt=True), bulkhead).call(slow)

    assert bulkhead.used == 1, "the worker still holds it; the thread cannot be killed"

    deadline = time.monotonic() + 2.0
    while bulkhead.used and time.monotonic() < deadline:
        time.sleep(0.01)
    assert bulkhead.used == 0, "released once the abandoned attempt returned"


def test_concurrent_executions_are_capped() -> None:
    """The whole point: never more than max_concurrency running at once."""
    bulkhead = Bulkhead(3, max_wait_time=5.0)
    fs = Failsafe(bulkhead)
    lock = threading.Lock()
    running = 0
    peak = 0

    def work() -> None:
        nonlocal running, peak
        with lock:
            running += 1
            peak = max(peak, running)
        time.sleep(0.01)
        with lock:
            running -= 1

    threads = [threading.Thread(target=lambda: fs.call(work)) for _ in range(12)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert peak == 3, "saturated, but never exceeded"
    assert bulkhead.used == 0


def test_async_and_sync_callers_share_one_permit_count() -> None:
    """D-14: one instance, one count, across both worlds."""
    bulkhead = Bulkhead(1)
    bulkhead.try_acquire_permit()

    async def scenario() -> None:
        with pytest.raises(BulkheadFull):
            await bulkhead.acquire_permit_async(timeout=0.01)

        bulkhead.release_permit()
        await bulkhead.acquire_permit_async(timeout=0.01)
        assert bulkhead.used == 1

    asyncio.run(scenario())


def test_async_executor_releases_its_permit() -> None:
    """The async driver honours the same finally."""
    bulkhead = Bulkhead(1)

    async def scenario() -> None:
        async def work() -> str:
            return "ok"

        assert await AsyncFailsafe(bulkhead).call(work) == "ok"
        assert bulkhead.used == 0

    asyncio.run(scenario())


def test_waiters_are_served_in_arrival_order() -> None:
    """FIFO fairness: a late arrival cannot barge ahead of a queue."""
    bulkhead = Bulkhead(1, max_wait_time=5.0)
    bulkhead.try_acquire_permit()
    order: list[int] = []
    lock = threading.Lock()

    def contend(index: int) -> None:
        bulkhead.acquire_permit()
        with lock:
            order.append(index)
        bulkhead.release_permit()

    threads = []
    for index in range(3):
        thread = threading.Thread(target=contend, args=(index,))
        thread.start()
        deadline = time.monotonic() + 2.0
        while bulkhead.waiters <= index and time.monotonic() < deadline:
            time.sleep(0.001)
        assert bulkhead.waiters == index + 1, "each thread queued before the next starts"
        threads.append(thread)

    bulkhead.release_permit()
    for thread in threads:
        thread.join()

    assert order == [0, 1, 2]


def test_invalid_configuration_is_rejected() -> None:
    """Constructor validation names the offending parameter."""
    with pytest.raises(ValueError, match="max_concurrency must be positive"):
        Bulkhead(0)
    with pytest.raises(ValueError, match="max_wait_time must not be negative"):
        Bulkhead(1, max_wait_time=-1.0)
