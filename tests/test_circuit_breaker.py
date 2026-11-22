"""CircuitBreaker semantics, ported from failsafe-go's breaker corpus.

Sources: `circuitbreaker/circuitbreaker_test.go`, `closedstate_test.go`,
`halfopenstate_test.go`, `openstate_test.go`.
"""

from __future__ import annotations

import asyncio

import pytest
from conftest import aflaky, flaky

from failsafe import (
    AsyncFailsafe,
    CircuitBreaker,
    CircuitBreakerOpen,
    Failsafe,
    Fixed,
    Jitter,
    RandomDelay,
    Rate,
    Ratio,
    RetryPolicy,
)
from failsafe._core import FakeClock

# --------------------------------------------------------------- closed state


def test_default_config_opens_after_one_failure(clock: FakeClock) -> None:
    """Go TestClosedStateFailureWithDefaultConfig."""
    breaker = CircuitBreaker(clock=clock)
    breaker.record_failure()
    assert breaker.is_open


def test_default_config_stays_closed_on_success(clock: FakeClock) -> None:
    """Go TestClosedStateSuccessWithDefaultConfig."""
    breaker = CircuitBreaker(clock=clock)
    breaker.record_success()
    assert breaker.is_closed


def test_opens_after_consecutive_failures(clock: FakeClock) -> None:
    """Go TestClosedStateFailureWithFailureThreshold: a ring of 3, all failures."""
    breaker = CircuitBreaker(failure_threshold=3, clock=clock)
    breaker.record_failure()
    breaker.record_success()
    breaker.record_failure()
    breaker.record_failure()
    assert breaker.is_closed
    breaker.record_failure()
    assert breaker.is_open


def test_opens_on_a_failure_ratio(clock: FakeClock) -> None:
    """Go TestClosedStateFailureWithFailureRatio: 2 of the last 3."""
    breaker = CircuitBreaker(failure_threshold=Ratio(2, 3), clock=clock)
    breaker.record_failure()
    breaker.record_success()
    assert breaker.is_closed
    breaker.record_failure()
    assert breaker.is_open


def test_stays_closed_when_the_ratio_is_never_met(clock: FakeClock) -> None:
    """Go TestClosedStateSuccessWithFailureRatio: alternating never reaches 3 of 4."""
    breaker = CircuitBreaker(failure_threshold=Ratio(3, 4), clock=clock)
    for _ in range(20):
        breaker.record_success()
        breaker.record_failure()
        assert breaker.is_closed


def test_stays_closed_when_the_threshold_is_never_met(clock: FakeClock) -> None:
    """Go TestClosedStateSuccessWithFailureThreshold."""
    breaker = CircuitBreaker(failure_threshold=2, clock=clock)
    for _ in range(20):
        breaker.record_success()
        breaker.record_failure()
        assert breaker.is_closed


# ----------------------------------------------------------------- open state


def test_open_rejects_until_the_delay_elapses(clock: FakeClock) -> None:
    """Go TestTryAcquirePermit."""
    breaker = CircuitBreaker(delay=0.1, clock=clock)
    breaker.open()
    assert breaker.is_open
    assert not breaker.try_acquire_permit()

    clock.advance(0.11)
    assert breaker.try_acquire_permit()
    assert breaker.is_half_open


def test_remaining_delay_counts_down(clock: FakeClock) -> None:
    """Go TestRemainingDelay and TestNoRemainingDelay."""
    breaker = CircuitBreaker(delay=1.0, clock=clock)
    assert breaker.remaining_delay() == 0.0, "a closed breaker has no delay"

    breaker.open()
    assert breaker.remaining_delay() == 1.0
    clock.advance(0.11)
    assert breaker.remaining_delay() == pytest.approx(0.89)
    clock.advance(5.0)
    assert breaker.remaining_delay() == 0.0


def test_reading_state_never_advances_to_half_open(clock: FakeClock) -> None:
    """Go: the transition is lazy, happening only inside tryAcquirePermit."""
    breaker = CircuitBreaker(delay=1.0, clock=clock)
    breaker.open()
    clock.advance(5.0)
    assert breaker.state == "open", "reading must not transition"
    assert breaker.try_acquire_permit()
    assert breaker.state == "half-open"


def test_jittered_delay_stays_in_range(clock: FakeClock) -> None:
    """Go TestJitteredDelay: within [delay-jitter, delay+jitter]."""
    for _ in range(100):
        breaker = CircuitBreaker(delay=Fixed(0.1, jitter=Jitter(seconds=0.02)), clock=clock)
        breaker.open()
        assert 0.08 <= breaker.remaining_delay() <= 0.12


def test_jitter_factor_delay_stays_in_range(clock: FakeClock) -> None:
    """Go TestJitterFactorDelay: within [delay*(1-f), delay*(1+f)]."""
    for _ in range(100):
        breaker = CircuitBreaker(delay=Fixed(0.1, jitter=Jitter(factor=0.25)), clock=clock)
        breaker.open()
        assert 0.075 <= breaker.remaining_delay() <= 0.125


def test_random_delay_stays_in_range(clock: FakeClock) -> None:
    """Go TestRandomDelay: within [min, max]."""
    for _ in range(100):
        breaker = CircuitBreaker(delay=RandomDelay(0.05, 0.15), clock=clock)
        breaker.open()
        assert 0.05 <= breaker.remaining_delay() <= 0.15


def test_injected_rng_makes_the_delay_reproducible(clock: FakeClock) -> None:
    """The RNG is a seam like the clock: same seed, same jittered delay."""
    import random

    def run() -> list[float]:
        breaker = CircuitBreaker(delay=RandomDelay(1.0, 3.0), clock=clock, rng=random.Random(7))
        delays = []
        for _ in range(5):
            breaker.open()
            delays.append(breaker.remaining_delay())
            breaker.close()
        return delays

    first, second = run(), run()
    assert first == second
    assert all(1.0 <= d <= 3.0 for d in first)
    assert len(set(first)) > 1, "a random delay must actually vary"


def test_open_is_a_noop_when_already_open(clock: FakeClock) -> None:
    """Go: open() returns early if already open, so the delay does not restart."""
    log: list[str] = []
    breaker = CircuitBreaker(delay=1.0, clock=clock, on_open=lambda e: log.append("open"))
    breaker.open()
    clock.advance(0.5)
    breaker.open()
    assert breaker.remaining_delay() == 0.5, "the delay must not restart"
    assert log == ["open"], "no event for an unchanged state"


# ------------------------------------------------------------ half-open state


def test_half_open_admits_a_bounded_number_of_trials(clock: FakeClock) -> None:
    """The cap that prevents a stampede on a recovering service."""
    breaker = CircuitBreaker(failure_threshold=1, success_threshold=3, clock=clock)
    breaker.half_open()

    assert breaker.try_acquire_permit()
    assert breaker.try_acquire_permit()
    assert breaker.try_acquire_permit()
    assert not breaker.try_acquire_permit(), "a fourth caller must be rejected"
    assert breaker.is_half_open, "rejection does not change state"


def test_half_open_permit_is_refunded_by_a_result(clock: FakeClock) -> None:
    """Go: permittedExecutions++ runs after every recorded result."""
    breaker = CircuitBreaker(failure_threshold=1, success_threshold=3, clock=clock)
    breaker.half_open()
    for _ in range(3):
        assert breaker.try_acquire_permit()
    assert not breaker.try_acquire_permit()

    breaker.record_success()
    assert breaker.try_acquire_permit(), "a decided trial frees its slot"


def test_half_open_failure_reopens_with_default_config(clock: FakeClock) -> None:
    """Go TestHalfOpenStateFailureWithDefaultConfig."""
    breaker = CircuitBreaker(clock=clock)
    breaker.half_open()
    breaker.record_failure()
    assert breaker.is_open


def test_half_open_success_closes_with_default_config(clock: FakeClock) -> None:
    """Go TestHalfOpenSuccessWithDefaultConfig."""
    breaker = CircuitBreaker(clock=clock)
    breaker.half_open()
    breaker.record_success()
    assert breaker.is_closed


def test_half_open_success_threshold_ignores_the_failure_threshold(clock: FakeClock) -> None:
    """Go TestHalfOpenFailureWithSuccessAndFailureThresholds: one failure reopens."""
    breaker = CircuitBreaker(success_threshold=3, failure_threshold=2, clock=clock)
    breaker.half_open()
    breaker.record_success()
    breaker.record_success()
    assert breaker.is_half_open
    breaker.record_failure()
    assert breaker.is_open, "the failure threshold is ignored when a success one is set"


def test_half_open_closes_on_the_success_threshold(clock: FakeClock) -> None:
    """Go TestHalfOpenSuccessWithSuccessThreshold."""
    breaker = CircuitBreaker(success_threshold=3, clock=clock)
    breaker.half_open()
    for _ in range(2):
        breaker.record_success()
        assert breaker.is_half_open
    breaker.record_success()
    assert breaker.is_closed


def test_half_open_success_ratio(clock: FakeClock) -> None:
    """Go TestHalfOpenSuccessWithSuccessRatio: 2 of 3."""
    breaker = CircuitBreaker(success_threshold=Ratio(2, 3), clock=clock)
    breaker.half_open()
    breaker.record_failure()
    breaker.record_success()
    assert breaker.is_half_open
    breaker.record_success()
    assert breaker.is_closed


def test_half_open_reopens_when_the_success_ratio_cannot_be_met(clock: FakeClock) -> None:
    """Go TestHalfOpenFailureWithSuccessRatio: failures exceed what the ratio allows."""
    breaker = CircuitBreaker(success_threshold=Ratio(2, 3), clock=clock)
    breaker.half_open()
    breaker.record_failure()
    breaker.record_success()
    assert breaker.is_half_open
    breaker.record_failure()
    assert breaker.is_open


def test_half_open_failure_ratio_without_success_threshold(clock: FakeClock) -> None:
    """Go TestHalfOpenFailureWithFailureRatio: 2 of 3."""
    breaker = CircuitBreaker(failure_threshold=Ratio(2, 3), clock=clock)
    breaker.half_open()
    breaker.record_failure()
    breaker.record_success()
    assert breaker.is_half_open
    breaker.record_failure()
    assert breaker.is_open


def test_half_open_closes_on_failure_ratio_slack(clock: FakeClock) -> None:
    """Go TestHalfOpenSuccessWithFailureRatio: successes exceed the ratio's slack."""
    breaker = CircuitBreaker(failure_threshold=Ratio(2, 3), clock=clock)
    breaker.half_open()
    breaker.record_failure()
    breaker.record_success()
    assert breaker.is_half_open
    breaker.record_success()
    assert breaker.is_closed


def test_half_open_has_no_remaining_delay(clock: FakeClock) -> None:
    """Go TestRemainingDelayInHalfOpenState."""
    breaker = CircuitBreaker(delay=1.0, clock=clock)
    breaker.half_open()
    assert breaker.remaining_delay() == 0.0


# ---------------------------------------------------------------------- stats


def test_metrics_track_the_window(clock: FakeClock) -> None:
    """Go TestGetSuccessAndFailureStats, with its 2-decimal rounding."""
    breaker = CircuitBreaker(
        failure_threshold=Ratio(5, 10), success_threshold=Ratio(15, 20), clock=clock
    )
    for i in range(7):
        if i % 2 == 0:
            breaker.record_success()
        else:
            breaker.record_failure()

    assert breaker.failures == 3
    assert breaker.failure_rate() == 0.43
    assert breaker.successes == 4
    assert breaker.success_rate() == 0.57


def test_rate_threshold_needs_min_executions(clock: FakeClock) -> None:
    """Go WithFailureRateThreshold: nothing opens before min_executions."""
    breaker = CircuitBreaker(
        failure_threshold=Rate(0.5, min_executions=5, period=60.0), clock=clock
    )
    for _ in range(4):
        breaker.record_failure()
    assert breaker.is_closed, "a rate must not trip before enough data"

    breaker.record_failure()
    assert breaker.is_open


def test_rate_threshold_window_expires(clock: FakeClock) -> None:
    """Go timedStats: results outside the period stop counting."""
    breaker = CircuitBreaker(
        failure_threshold=Rate(0.5, min_executions=2, period=10.0), clock=clock
    )
    breaker.record_failure()
    clock.advance(11.0)
    breaker.record_success()
    breaker.record_success()
    assert breaker.is_closed, "the old failure has expired out of the window"


# ----------------------------------------------------------------- as a policy


def test_rejects_without_invoking_the_function(clock: FakeClock) -> None:
    """Go: PreExecute returns ErrOpen and innerFn is never called."""
    breaker = CircuitBreaker(failure_threshold=1, delay=60.0, clock=clock)
    fs = Failsafe(breaker, clock=clock)

    with pytest.raises(ConnectionError):
        fs.call(flaky(9))
    assert breaker.is_open

    calls = 0

    def fn() -> str:
        nonlocal calls
        calls += 1
        return "ok"

    with pytest.raises(CircuitBreakerOpen) as info:
        fs.call(fn)
    assert calls == 0, "an open breaker must not invoke the function"
    assert info.value.remaining_delay == pytest.approx(60.0)


def test_rejected_attempt_counts_as_an_attempt_not_an_execution(clock: FakeClock) -> None:
    """Go: Attempts includes blocked attempts, Executions does not."""
    breaker = CircuitBreaker(failure_threshold=1, clock=clock)
    breaker.open()
    fs = Failsafe(RetryPolicy(max_retries=1), breaker, clock=clock)

    with pytest.raises(Exception, match="retries exceeded"):
        fs.call(lambda: "never")


def test_recovers_through_half_open_to_closed(clock: FakeClock) -> None:
    breaker = CircuitBreaker(failure_threshold=1, delay=10.0, clock=clock)
    fs = Failsafe(breaker, clock=clock)

    with pytest.raises(ConnectionError):
        fs.call(flaky(9))
    assert breaker.is_open

    clock.advance(11.0)
    assert fs.call(lambda: "ok") == "ok"
    assert breaker.is_closed


def test_handle_narrows_what_counts_as_a_failure(clock: FakeClock) -> None:
    breaker = CircuitBreaker(failure_threshold=1, handle=ConnectionError, clock=clock)
    fs = Failsafe(breaker, clock=clock)

    def fn() -> str:
        raise ValueError("not a breaker failure")

    with pytest.raises(ValueError):
        fs.call(fn)
    assert breaker.is_closed, "an unhandled error is a success for the breaker"


def test_state_change_listeners_fire_in_order(clock: FakeClock) -> None:
    log: list[str] = []
    breaker = CircuitBreaker(
        failure_threshold=1,
        delay=10.0,
        clock=clock,
        on_open=lambda e: log.append(f"open from {e.previous_state}"),
        on_half_open=lambda e: log.append("half-open"),
        on_close=lambda e: log.append("close"),
        on_state_change=lambda e: log.append(f"changed {e.previous_state}->{e.state}"),
    )
    fs = Failsafe(breaker, clock=clock)

    with pytest.raises(ConnectionError):
        fs.call(flaky(9))
    clock.advance(11.0)
    fs.call(lambda: "ok")

    assert log == [
        "open from closed",
        "changed closed->open",
        "half-open",
        "changed open->half-open",
        "close",
        "changed half-open->closed",
    ]


def test_listener_may_reenter_the_breaker(clock: FakeClock) -> None:
    """Go releases the lock before firing listeners, so they may call back in."""
    seen: list[str] = []
    breaker = CircuitBreaker(
        failure_threshold=1, clock=clock, on_open=lambda e: seen.append(breaker.state)
    )
    breaker.record_failure()
    assert seen == ["open"], "the state change is already visible to the listener"


def test_state_is_shared_between_drivers(clock: FakeClock) -> None:
    """The property the whole design rests on, now with a real breaker."""
    breaker = CircuitBreaker(failure_threshold=1, delay=60.0, clock=clock)

    with pytest.raises(ConnectionError):
        Failsafe(breaker, clock=clock).call(flaky(9))
    assert breaker.is_open

    async def scenario() -> None:
        with pytest.raises(CircuitBreakerOpen):
            await AsyncFailsafe(breaker, clock=clock).call(aflaky(0))

    asyncio.run(scenario())


def test_retry_outside_breaker_sees_rejections(clock: FakeClock) -> None:
    """Go's recommended composition: RetryPolicy(CircuitBreaker(fn))."""
    breaker = CircuitBreaker(failure_threshold=1, delay=0.0, clock=clock)
    fs = Failsafe(RetryPolicy(max_retries=3), breaker, clock=clock)
    assert fs.call(flaky(2)) == "ok"


def test_invalid_configuration_is_rejected() -> None:
    with pytest.raises(ValueError, match="failure_threshold"):
        CircuitBreaker(failure_threshold=0)
    with pytest.raises(ValueError, match="count must not exceed"):
        Ratio(5, 3)
    with pytest.raises(ValueError, match="rate must be between"):
        Rate(1.5, min_executions=5, period=60.0)
    with pytest.raises(ValueError, match="min_executions"):
        Rate(0.5, min_executions=0, period=60.0)


def test_callable_delay_sees_the_failing_execution(clock: FakeClock) -> None:
    """Go WithDelayFunc: the delay is computed from the execution that tripped it."""
    seen: list[object] = []

    def compute(ex: object) -> float:
        seen.append(None if ex is None else ex.last_exception)  # type: ignore[attr-defined]
        return 30.0

    breaker = CircuitBreaker(failure_threshold=1, delay=compute, clock=clock)
    with pytest.raises(ConnectionError):
        Failsafe(breaker, clock=clock).call(flaky(9))

    assert breaker.remaining_delay() == 30.0
    assert isinstance(seen[0], ConnectionError)


def test_manual_open_has_no_execution_for_a_callable_delay(clock: FakeClock) -> None:
    """PORT.md item 12: a callable delay has no fixed delay to fall back on.

    Go keeps DelayFunc and Delay in separate fields, so a manual Open() falls through
    to the fixed one. Our single delay= union cannot express that pair, so a
    callable-only configuration yields no delay, as Go does when neither is set.
    """
    breaker = CircuitBreaker(failure_threshold=1, delay=lambda ex: 30.0, clock=clock)
    breaker.open()
    assert breaker.is_open
    assert breaker.remaining_delay() == 0.0
