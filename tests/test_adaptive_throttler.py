"""Ported from failsafe-go's adaptivethrottler/adaptivethrottler_test.go.

Go's probabilistic tests loop until a rejection happens, accepting a documented
0.01% flake rate. We inject the RNG instead, so the same assertions are exact.
"""

from __future__ import annotations

import asyncio
import contextlib
import random

import pytest

from failsafe import AdaptiveThrottler, AsyncFailsafe, Failsafe, RetryPolicy, ThrottleExceeded
from failsafe._core import FakeClock, Outcome
from failsafe.adaptive_throttler import _compute_rejection_rate


class SequenceRandom(random.Random):
    """An RNG returning a fixed sequence, so rejection decisions are exact."""

    def __init__(self, values: list[float]) -> None:
        super().__init__()
        self._values = list(values)

    def random(self) -> float:
        return self._values.pop(0) if self._values else 1.0


def record_failures(throttler: AdaptiveThrottler, count: int) -> None:
    for _ in range(count):
        throttler.record_failure()


def test_calculates_rejection_rate_after_failures() -> None:
    """Go: should calculate rejection rate after failures."""
    throttler = AdaptiveThrottler()
    throttler.acquire_permit()

    throttler.record_failure()
    with contextlib.suppress(ThrottleExceeded):
        throttler.acquire_permit()

    assert throttler.rejection_rate == 0.5


def test_raises_throttle_exceeded_when_rejected() -> None:
    """Go loops 50 times hoping for a rejection; a draw below the rate is certain here."""
    throttler = AdaptiveThrottler(
        max_rejection_rate=1.0,
        failure_rate_threshold=0.1,
        execution_threshold=1,
        rng=SequenceRandom([0.0]),
    )
    record_failures(throttler, 50)

    with pytest.raises(ThrottleExceeded):
        throttler.acquire_permit()
    assert throttler.rejection_rate == pytest.approx(50 / 51)


def test_respects_max_rejection_rate() -> None:
    """Go: should respect max rejection rate."""
    throttler = AdaptiveThrottler(max_rejection_rate=0.5)
    record_failures(throttler, 20)
    throttler.try_acquire_permit()

    # Uncapped this would be 20/21 = .95; the cap is what holds it down.
    assert throttler.rejection_rate == 0.5


def test_does_not_reject_below_the_execution_threshold() -> None:
    """Go: should not reject when execution threshold is not met.

    One failure leaves the throttler under the threshold of 2, so the permit is
    granted whatever the RNG says. A second failure meets it, and the draw decides.
    """
    throttler = AdaptiveThrottler(
        failure_rate_threshold=0.1,
        execution_threshold=2,
        rng=SequenceRandom([0.0, 0.0]),
    )
    record_failures(throttler, 1)
    assert throttler.try_acquire_permit()

    record_failures(throttler, 1)
    assert not throttler.try_acquire_permit()


def test_never_rejects_everything_by_default() -> None:
    """max_rejection_rate defaults to .9, so a recovering backend still gets probes."""
    throttler = AdaptiveThrottler(execution_threshold=1, rng=SequenceRandom([0.95]))
    record_failures(throttler, 1000)

    assert throttler.try_acquire_permit()
    assert throttler.rejection_rate == 0.9


def test_successes_lower_the_rejection_rate() -> None:
    """Recorded successes raise the allowance, so the rate falls back towards zero."""
    throttler = AdaptiveThrottler(execution_threshold=1)
    record_failures(throttler, 10)
    throttler.try_acquire_permit()
    throttled = throttler.rejection_rate

    for _ in range(90):
        throttler.record_success()
    throttler.try_acquire_permit()

    assert throttled > 0
    assert throttler.rejection_rate == 0


def test_results_age_out_of_the_thresholding_period(clock: FakeClock) -> None:
    """The window is rolling: failures stop counting once the period passes."""
    throttler = AdaptiveThrottler(execution_threshold=1, thresholding_period=60.0, clock=clock)
    record_failures(throttler, 20)
    throttler.try_acquire_permit()
    assert throttler.rejection_rate > 0

    clock.advance(61.0)
    throttler.record_success()
    throttler.try_acquire_permit()

    assert throttler.executions == 1
    assert throttler.rejection_rate == 0


def test_results_age_out_gradually(clock: FakeClock) -> None:
    """Buckets expire one at a time, so the window slides rather than resetting."""
    throttler = AdaptiveThrottler(thresholding_period=20.0, clock=clock)
    for _ in range(10):
        throttler.record_failure()
        clock.advance(1.0)
    assert throttler.executions == 10

    clock.advance(11.0)
    throttler.record_failure()

    # 20s over 20 buckets: the two oldest seconds expired, the new failure counts.
    assert throttler.executions == 9


def test_record_result_uses_the_handle_condition() -> None:
    """handle= decides what counts as a failure, as for the other failure policies."""
    throttler = AdaptiveThrottler(handle=ValueError)
    throttler.record_result(Outcome(exc=ValueError("boom")))
    throttler.record_result(Outcome(exc=TypeError("fine")))

    assert throttler.failures == 1
    assert throttler.successes == 1


def test_policy_rejects_without_invoking_the_function() -> None:
    """A rejected execution must not reach the wrapped callable."""
    throttler = AdaptiveThrottler(
        execution_threshold=1, max_rejection_rate=1.0, rng=SequenceRandom([0.0])
    )
    record_failures(throttler, 50)
    calls = 0

    def fn() -> str:
        nonlocal calls
        calls += 1
        return "ok"

    with pytest.raises(ThrottleExceeded):
        Failsafe(throttler).call(fn)
    assert calls == 0


def test_policy_records_outcomes() -> None:
    """Executions run through the policy feed the window."""
    throttler = AdaptiveThrottler()

    Failsafe(throttler).call(lambda: "ok")
    with pytest.raises(ConnectionError):
        Failsafe(throttler).call(_raise)

    assert throttler.successes == 1
    assert throttler.failures == 1


def test_policy_fires_listeners() -> None:
    events: list[str] = []
    throttler = AdaptiveThrottler(
        on_success=lambda e: events.append("success"),
        on_failure=lambda e: events.append("failure"),
    )

    Failsafe(throttler).call(lambda: "ok")
    with pytest.raises(ConnectionError):
        Failsafe(throttler).call(_raise)

    assert events == ["success", "failure"]


def test_async_shares_state_with_sync() -> None:
    """One throttler instance drives both drivers and shares its window."""
    throttler = AdaptiveThrottler()

    async def scenario() -> None:
        await AsyncFailsafe(throttler).call(_async_ok)

    Failsafe(throttler).call(lambda: "ok")
    asyncio.run(scenario())

    assert throttler.successes == 2


def test_acquire_permit_async_matches_sync() -> None:
    """The same object offers both forms, so callers need not know which."""
    throttler = AdaptiveThrottler(
        execution_threshold=1, max_rejection_rate=1.0, rng=SequenceRandom([0.0])
    )
    record_failures(throttler, 50)

    async def scenario() -> None:
        with pytest.raises(ThrottleExceeded):
            await throttler.acquire_permit_async()

    asyncio.run(scenario())


def test_retry_sees_throttle_rejection_as_a_failure() -> None:
    """An outer retry may retry a rejection, as Go returns it as an ordinary error."""
    throttler = AdaptiveThrottler(
        execution_threshold=1, max_rejection_rate=1.0, rng=SequenceRandom([0.0, 0.0, 1.0])
    )
    record_failures(throttler, 50)
    calls = 0
    attempts = 0

    def _bump() -> None:
        nonlocal attempts
        attempts += 1

    def fn() -> str:
        nonlocal calls
        calls += 1
        return "ok"

    retry: RetryPolicy = RetryPolicy(delay=None, on_retry=lambda e: _bump())
    assert Failsafe(retry, throttler).call(fn) == "ok"
    # Two rejections were retried; only the third attempt reached the function.
    assert calls == 1
    assert attempts == 2


@pytest.mark.parametrize("bad", [-0.1, 1.1])
def test_rejects_out_of_range_rates(bad: float) -> None:
    with pytest.raises(ValueError):
        AdaptiveThrottler(failure_rate_threshold=bad)
    with pytest.raises(ValueError):
        AdaptiveThrottler(max_rejection_rate=bad)


def test_rejects_a_non_positive_period() -> None:
    with pytest.raises(ValueError):
        AdaptiveThrottler(thresholding_period=0)


def _raise() -> str:
    raise ConnectionError("boom")


async def _async_ok() -> str:
    return "ok"


class TestComputeRejectionRate:
    """Go's TestComputeRejectionRate, ported case for case."""

    def test_better_than_the_threshold(self) -> None:
        assert _compute_rejection_rate(100.0, 95.0, 0.9, 0.8, 1) == 0.0

    def test_slightly_overloaded(self) -> None:
        # maxAllowed = 90/0.9 = 100; excess = 20; 20/121
        assert _compute_rejection_rate(120.0, 90.0, 0.9, 0.8, 1) == pytest.approx(
            20.0 / 121.0, abs=0.001
        )

    def test_heavily_overloaded(self) -> None:
        result = _compute_rejection_rate(200.0, 50.0, 0.9, 0.8, 1)
        assert 0.7 < result < 0.8

    def test_capped_at_the_max(self) -> None:
        assert _compute_rejection_rate(1000.0, 10.0, 0.9, 0.8, 1) == 0.8

    def test_zero_executions(self) -> None:
        assert _compute_rejection_rate(0.0, 0.0, 0.9, 0.8, 1) == 0.0

    def test_zero_successes(self) -> None:
        assert _compute_rejection_rate(100.0, 0.0, 0.9, 0.8, 1) == 0.8

    def test_decreasing_success_rates(self) -> None:
        healthy = _compute_rejection_rate(100.0, 90.0, 0.9, 0.8, 1)
        struggling = _compute_rejection_rate(100.0, 70.0, 0.9, 0.8, 1)
        unhealthy = _compute_rejection_rate(100.0, 50.0, 0.9, 0.8, 1)

        assert healthy == 0.0
        assert struggling > healthy
        assert unhealthy > struggling
