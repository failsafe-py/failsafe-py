"""RateLimiter semantics, ported from failsafe-go's ratelimiter corpus.

Go's stats tests drive a ``TestStopwatch`` directly; here :class:`FakeClock` plays
that part, and wait times are asserted in milliseconds to match Go's tables.
"""

from __future__ import annotations

import asyncio
import threading
import time

import pytest

from failsafe import AsyncFailsafe, Failsafe, RateLimiter, RateLimitExceeded, RetryPolicy
from failsafe._core import FakeClock
from failsafe.rate_limiter import _NANOS, _BurstyStats, _SmoothStats


def ms(limiter: RateLimiter, permits: int = 1) -> int:
    """Reserve permits and return the wait in whole milliseconds, as Go's tests do."""
    return round(limiter.reserve_permit(permits) * 1000)


def ms_n(limiter: RateLimiter, permits: int, times: int) -> int:
    """Reserve ``permits`` ``times`` over, returning the last wait."""
    wait = 0
    for _ in range(times):
        wait = ms(limiter, permits)
    return wait


def at(clock: FakeClock, millis: float) -> None:
    """Move the clock to an absolute offset in milliseconds from its start."""
    clock.advance(millis / 1000 - clock.now())


def at_nanos(clock: FakeClock, nanos: float) -> None:
    """Move the clock to an absolute offset in nanoseconds from its start."""
    clock.advance(nanos / 1e9 - clock.now())


def next_free_ms(limiter: RateLimiter) -> int:
    """The smooth limiter's next free permit time, in milliseconds."""
    stats = limiter._stats
    assert isinstance(stats, _SmoothStats)
    return round(stats._next_free / _NANOS * 1000)


def bursty_stats(limiter: RateLimiter) -> _BurstyStats:
    """The bursty limiter's internals, which Go's corpus asserts against directly."""
    stats = limiter._stats
    assert isinstance(stats, _BurstyStats)
    return stats


def test_smooth_acquire_permits(clock: FakeClock) -> None:
    """Go TestSmoothAcquirePermits: wait times and next-free time over a timeline."""
    limiter = RateLimiter.smooth(max_rate=0.5, clock=clock)

    assert ms_n(limiter, 1, 7) == 3000
    assert next_free_ms(limiter) == 3500

    at(clock, 800)
    assert ms(limiter, 3) == 3700
    assert next_free_ms(limiter) == 5000

    at(clock, 2300)
    assert ms(limiter, 1) == 2700
    assert next_free_ms(limiter) == 5500

    at(clock, 3500)
    assert ms(limiter, 3) == 3000
    assert next_free_ms(limiter) == 7000

    at(clock, 9100)
    assert ms(limiter, 3) == 900
    assert next_free_ms(limiter) == 10500

    at(clock, 11000)
    assert ms(limiter, 1) == 0
    assert next_free_ms(limiter) == 11500


def test_smooth_acquire_permits_at_200ms(clock: FakeClock) -> None:
    """Go TestSmoothAcquirePermits, second half: 1 permit every 200ms."""
    limiter = RateLimiter.smooth(max_rate=0.2, clock=clock)

    assert ms(limiter, 3) == 400
    assert next_free_ms(limiter) == 600

    at(clock, 550)
    assert ms(limiter, 2) == 250
    assert next_free_ms(limiter) == 1000

    at(clock, 2210)
    assert ms(limiter, 2) == 190
    assert next_free_ms(limiter) == 2600


def test_smooth_next_free_time_is_consistent(clock: FakeClock) -> None:
    """Go's assertSmoothRateLimiterStats invariant: next free == elapsed + wait + interval."""
    limiter = RateLimiter.smooth(max_rate=0.5, clock=clock)
    for elapsed in (0, 800, 2300, 3500, 9100, 11000):
        at(clock, elapsed)
        wait = ms(limiter, 2)
        assert next_free_ms(limiter) == elapsed + wait + 500


def test_smooth_initial_stats(clock: FakeClock) -> None:
    """Go TestSmoothAcquireInitialStats: the first permit is always free."""
    limiter = RateLimiter.smooth(max_rate=100e-9, clock=clock)

    assert limiter.reserve_permit() == 0
    assert limiter.reserve_permit() == pytest.approx(100e-9)
    at_nanos(clock, 100)
    assert limiter.reserve_permit() == pytest.approx(100e-9)
    assert limiter.reserve_permit() == pytest.approx(200e-9)


def test_smooth_initial_stats_with_gaps(clock: FakeClock) -> None:
    """Go TestSmoothAcquireInitialStats, second half: idle time frees permits."""
    limiter = RateLimiter.smooth(max_rate=100e-9, clock=clock)

    assert limiter.reserve_permit() == 0
    at_nanos(clock, 150)
    assert limiter.reserve_permit() == 0
    at_nanos(clock, 250)
    assert limiter.reserve_permit(2) == pytest.approx(50e-9)


def test_bursty_acquire_permits(clock: FakeClock) -> None:
    """Go TestBurstyAcquirePermits: 2 permits per second, tracked into deficit."""
    limiter = RateLimiter.bursty(2, 1.0, clock=clock)
    stats = bursty_stats(limiter)

    assert ms_n(limiter, 1, 7) == 3000
    assert stats._available == -5
    assert stats._current_period == 0

    at(clock, 800)
    assert ms(limiter, 3) == 3200
    assert stats._available == -8
    assert stats._current_period == 0

    at(clock, 2300)
    assert ms(limiter, 1) == 2700
    assert stats._available == -5
    assert stats._current_period == 2

    at(clock, 3500)
    assert ms_n(limiter, 1, 3) == 2500
    assert stats._available == -6
    assert stats._current_period == 3

    at(clock, 7000)
    assert ms(limiter, 1) == 0
    assert stats._available == 1
    assert stats._current_period == 7


def test_bursty_acquire_permits_with_five_per_second(clock: FakeClock) -> None:
    """Go TestBurstyAcquirePermits, second half: 5 permits per second."""
    limiter = RateLimiter.bursty(5, 1.0, clock=clock)
    stats = bursty_stats(limiter)

    at(clock, 300)
    assert ms(limiter, 3) == 0
    assert stats._available == 2
    assert stats._current_period == 0

    at(clock, 1550)
    assert ms(limiter, 10) == 450
    assert stats._available == -5
    assert stats._current_period == 1

    at(clock, 2210)
    assert ms(limiter, 2) == 790, "must wait till the next period"
    assert stats._available == -2
    assert stats._current_period == 2


@pytest.mark.parametrize("flavour", ["smooth", "bursty"])
def test_permits_are_acquired_equally_one_at_a_time_or_together(flavour: str) -> None:
    """Go TestShouldAcquirePermitsEqually: N at once equals N one at a time."""

    def build() -> tuple[RateLimiter, FakeClock]:
        c = FakeClock()
        if flavour == "smooth":
            return RateLimiter.smooth(max_rate=500.0, clock=c), c
        return RateLimiter.bursty(2, 1.0, clock=c), c

    one, clock_one = build()
    many, clock_many = build()
    assert one.reserve_permit(7) == ms_n_raw(many, 7)

    at_nanos(clock_one, 2700)
    at_nanos(clock_many, 2700)
    assert one.reserve_permit(5) == ms_n_raw(many, 5)


def ms_n_raw(limiter: RateLimiter, times: int) -> float:
    """Reserve one permit ``times`` over, returning the last wait in seconds."""
    wait = 0.0
    for _ in range(times):
        wait = limiter.reserve_permit()
    return wait


@pytest.mark.parametrize("flavour", ["smooth", "bursty"])
def test_first_permit_has_zero_wait(flavour: str, clock: FakeClock) -> None:
    """Go TestShouldHaveZeroWaitTime."""
    if flavour == "smooth":
        limiter = RateLimiter.smooth(max_rate=500.0, clock=clock)
    else:
        limiter = RateLimiter.bursty(2, 1.0, clock=clock)
    assert limiter.reserve_permit() == 0


def test_smooth_accepts_max_executions_per_period(clock: FakeClock) -> None:
    """``smooth(100, 1.0)`` and ``smooth(max_rate=0.01)`` describe the same limiter."""
    by_count = RateLimiter.smooth(100, 1.0, clock=clock)
    by_rate = RateLimiter.smooth(max_rate=0.01, clock=clock)
    assert ms_n_raw(by_count, 5) == ms_n_raw(by_rate, 5)


def test_try_acquire_permit_only_takes_free_permits(clock: FakeClock) -> None:
    """A permit that would require waiting is not taken."""
    limiter = RateLimiter.bursty(2, 1.0, clock=clock)
    assert limiter.try_acquire_permit()
    assert limiter.try_acquire_permit()
    assert not limiter.try_acquire_permit()

    at(clock, 1000)
    assert limiter.try_acquire_permit(), "a new period refills the allowance"


def test_try_acquire_permit_does_not_consume_on_refusal(clock: FakeClock) -> None:
    """A refused acquisition must not reserve anything, or callers would starve."""
    limiter = RateLimiter.bursty(1, 1.0, clock=clock)
    assert limiter.try_acquire_permit()
    for _ in range(5):
        assert not limiter.try_acquire_permit()

    at(clock, 1000)
    assert limiter.try_acquire_permit(), "the refusals must not have built a deficit"


def test_try_reserve_permit_refuses_a_wait_that_is_too_long(clock: FakeClock) -> None:
    """Go's TryReservePermits: -1 when the wait exceeds the max."""
    limiter = RateLimiter.smooth(max_rate=1.0, clock=clock)
    assert limiter.try_reserve_permit(0.5) == 0, "the first permit is free"
    assert limiter.try_reserve_permit(0.5) == -1.0, "the second needs a full second"
    assert limiter.try_reserve_permit(2.0) == pytest.approx(1.0)


def test_reserve_permit_always_reserves(clock: FakeClock) -> None:
    """ReservePermit has no ceiling: the wait grows without refusing."""
    limiter = RateLimiter.smooth(max_rate=1.0, clock=clock)
    assert [limiter.reserve_permit() for _ in range(4)] == [0, 1.0, 2.0, 3.0]


def test_reset_discards_reservations(clock: FakeClock) -> None:
    """Go's Reset."""
    limiter = RateLimiter.smooth(max_rate=1.0, clock=clock)
    for _ in range(5):
        limiter.reserve_permit()
    limiter.reset()
    assert limiter.reserve_permit() == 0


def test_executor_rejects_when_no_permit_is_free(clock: FakeClock) -> None:
    """With the default zero max wait, a limited execution fails rather than waits."""
    events: list[str] = []
    limiter = RateLimiter.bursty(
        1, 1.0, clock=clock, on_rate_limit_exceeded=lambda _e: events.append("limited")
    )
    fs = Failsafe(limiter, clock=clock)

    assert fs.call(lambda: "ok") == "ok"
    with pytest.raises(RateLimitExceeded):
        fs.call(lambda: "ok")
    assert events == ["limited"]


def test_executor_waits_up_to_max_wait_time(clock: FakeClock) -> None:
    """A permit that arrives within max_wait_time is waited for, not rejected."""
    limiter = RateLimiter.smooth(max_rate=1.0, max_wait_time=2.0, clock=clock)
    fs = Failsafe(limiter, clock=clock)

    assert fs.call(lambda: "a") == "a"
    assert fs.call(lambda: "b") == "b"
    assert clock.slept == [1.0], "the second execution waited for its permit"


def test_executor_rejects_a_wait_beyond_max_wait_time(clock: FakeClock) -> None:
    """Standalone reservations push the next permit past the executor's patience."""
    limiter = RateLimiter.smooth(max_rate=1.0, max_wait_time=2.0, clock=clock)
    fs = Failsafe(limiter, clock=clock)

    for _ in range(4):
        limiter.reserve_permit()
    assert limiter.reserve_permit() == pytest.approx(4.0), "the queue is 4s deep"

    with pytest.raises(RateLimitExceeded, match=r"max wait: 2\.000s"):
        fs.call(lambda: "c")
    assert clock.slept == [], "a rejected execution never waits"


def test_max_wait_time_does_not_apply_to_standalone_acquire(clock: FakeClock) -> None:
    """Go documents the asymmetry: max_wait_time is an executor-only setting."""
    limiter = RateLimiter.smooth(max_rate=1.0, max_wait_time=0.0, clock=clock)

    limiter.acquire_permit()
    limiter.acquire_permit()

    assert clock.slept == [1.0], "standalone waited rather than raising"


def test_async_executor_waits_for_a_permit() -> None:
    """The async driver services the same Sleep instruction."""
    limiter = RateLimiter.smooth(max_rate=0.02, max_wait_time=1.0)
    fs = AsyncFailsafe(limiter)

    async def scenario() -> list[str]:
        async def call(tag: str) -> str:
            return tag

        started = time.monotonic()
        results = [await fs.call(call, "a"), await fs.call(call, "b")]
        assert time.monotonic() - started >= 0.02
        return results

    assert asyncio.run(scenario()) == ["a", "b"]


def test_async_standalone_acquire_waits() -> None:
    """acquire_permit_async suspends rather than blocking the loop."""
    limiter = RateLimiter.smooth(max_rate=0.02)

    async def scenario() -> None:
        await limiter.acquire_permit_async()
        started = time.monotonic()
        await limiter.acquire_permit_async()
        assert time.monotonic() - started >= 0.02

    asyncio.run(scenario())


def test_retry_outside_a_limiter_reacquires_per_attempt(clock: FakeClock) -> None:
    """Each attempt takes its own permit, so a retry is itself rate limited."""
    limiter = RateLimiter.smooth(max_rate=1.0, max_wait_time=5.0, clock=clock)
    attempts = 0

    def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ConnectionError("boom")
        return "ok"

    fs = Failsafe(RetryPolicy(max_retries=3, delay=None), limiter, clock=clock)
    assert fs.call(flaky) == "ok"
    assert attempts == 3
    assert clock.slept == [1.0, 1.0], "attempts 2 and 3 each waited for a permit"


def test_concurrent_reservations_do_not_overlap() -> None:
    """The reservation arithmetic is atomic: no two threads get the same slot."""
    limiter = RateLimiter.smooth(max_rate=0.001)
    waits: list[float] = []
    lock = threading.Lock()

    def reserve() -> None:
        wait = limiter.reserve_permit()
        with lock:
            waits.append(wait)

    threads = [threading.Thread(target=reserve) for _ in range(20)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(set(waits)) == 20, "every caller got a distinct slot"


def test_invalid_configuration_is_rejected() -> None:
    """Constructor validation names the offending parameter."""
    with pytest.raises(ValueError, match="exactly one of max_executions or max_rate"):
        RateLimiter.smooth()
    with pytest.raises(ValueError, match="exactly one of max_executions or max_rate"):
        RateLimiter.smooth(10, max_rate=0.1)
    with pytest.raises(ValueError, match="max_executions must be positive"):
        RateLimiter.smooth(0)
    with pytest.raises(ValueError, match="max_executions must be positive"):
        RateLimiter.bursty(0)
    with pytest.raises(ValueError, match="interval between executions must be positive"):
        RateLimiter.smooth(max_rate=0.0)
    with pytest.raises(ValueError, match="permits must be positive"):
        RateLimiter.bursty(1).reserve_permit(0)
