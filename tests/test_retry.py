"""RetryPolicy semantics, ported from failsafe-go's retry corpus.

Sources: `retrypolicy/retry_test.go`, `retrypolicy/retryexecutor_test.go`, and the
behaviours `retryexecutor.go` encodes that those files assume.
"""

from __future__ import annotations

import asyncio

import pytest
from conftest import aflaky, flaky

from failsafe import (
    AsyncFailsafe,
    Backoff,
    Failsafe,
    Fixed,
    Jitter,
    RandomDelay,
    Result,
    RetriesExceeded,
    RetryPolicy,
    current_execution,
)
from failsafe._core import FakeClock


def test_defaults_allow_three_attempts(clock: FakeClock) -> None:
    """Go: NewBuilder sets defaultMaxRetries = 2, so 3 attempts."""
    fn = flaky(2)
    assert Failsafe(RetryPolicy(), clock=clock).call(fn) == "ok"

    with pytest.raises(RetriesExceeded):
        Failsafe(RetryPolicy(), clock=clock).call(flaky(3))


def test_retries_exceeded_carries_the_last_failure(clock: FakeClock) -> None:
    """Go: ExceededError{LastResult, LastError}, with Unwrap exposing the cause."""
    with pytest.raises(RetriesExceeded) as info:
        Failsafe(RetryPolicy(max_retries=1), clock=clock).call(flaky(9))

    assert isinstance(info.value.last_exception, ConnectionError)
    assert info.value.last_result is None
    assert isinstance(info.value.__cause__, ConnectionError)


def test_return_last_failure_skips_the_wrapper(clock: FakeClock) -> None:
    """Go: returnLastFailure returns the last failure as-is."""
    policy = RetryPolicy(max_retries=1, return_last_failure=True)
    with pytest.raises(ConnectionError, match="boom 2"):
        Failsafe(policy, clock=clock).call(flaky(9))


def test_max_retries_zero_still_wraps(clock: FakeClock) -> None:
    """Go: failedAttempts (1) > maxRetries (0) exceeds on the first failure."""
    calls = 0

    def fn() -> str:
        nonlocal calls
        calls += 1
        raise ConnectionError("boom")

    with pytest.raises(RetriesExceeded):
        Failsafe(RetryPolicy(max_retries=0), clock=clock).call(fn)
    assert calls == 1


def test_unlimited_retries_until_success(clock: FakeClock) -> None:
    assert Failsafe(RetryPolicy(max_retries=None), clock=clock).call(flaky(20)) == "ok"


def test_negative_max_retries_is_rejected() -> None:
    with pytest.raises(ValueError, match="max_retries"):
        RetryPolicy(max_retries=-1)


def test_backoff_doubles_and_caps(clock: FakeClock) -> None:
    """Go TestGetFixedOrRandomDelay: 2s backoff factor 2, max 30s."""
    policy = RetryPolicy(delay=Backoff(2.0, 30.0, factor=2.0), max_retries=None)
    Failsafe(policy, clock=clock).call(flaky(5))
    assert clock.slept == [2.0, 4.0, 8.0, 16.0, 30.0]


def test_backoff_state_is_per_execution(clock: FakeClock) -> None:
    """A shared policy must not carry one execution's backoff into the next."""
    policy = RetryPolicy(delay=Backoff(1.0, 60.0), max_retries=None)
    first, second = FakeClock(), FakeClock()

    Failsafe(policy, clock=first).call(flaky(3))
    Failsafe(policy, clock=second).call(flaky(3))

    assert first.slept == [1.0, 2.0, 4.0]
    assert second.slept == first.slept


def test_fixed_delay_repeats(clock: FakeClock) -> None:
    Failsafe(RetryPolicy(delay=2.5, max_retries=None), clock=clock).call(flaky(3))
    assert clock.slept == [2.5, 2.5, 2.5]


def test_timedelta_delay_is_accepted(clock: FakeClock) -> None:
    from datetime import timedelta

    policy = RetryPolicy(delay=timedelta(milliseconds=500), max_retries=None)
    Failsafe(policy, clock=clock).call(flaky(2))
    assert clock.slept == [0.5, 0.5]


def test_random_delay_stays_in_range(clock: FakeClock) -> None:
    """Go TestRandomDelay: delay within [min, max]."""
    policy = RetryPolicy(delay=RandomDelay(1.0, 3.0), max_retries=None)
    Failsafe(policy, clock=clock).call(flaky(20))
    assert len(clock.slept) == 20
    assert all(1.0 <= slept <= 3.0 for slept in clock.slept)


def test_injected_rng_makes_delays_reproducible(clock: FakeClock) -> None:
    """The RNG is a seam like the clock: same seed, same delays, no seed-and-hope."""
    import random

    def run() -> list[float]:
        local = FakeClock()
        policy = RetryPolicy(delay=RandomDelay(1.0, 3.0), max_retries=None, rng=random.Random(7))
        Failsafe(policy, clock=local).call(flaky(10))
        return local.slept

    first, second = run(), run()
    assert first == second
    assert len(set(first)) > 1, "a random delay must actually vary"


def test_jitter_factor_stays_in_range(clock: FakeClock) -> None:
    """Go TestJitterFactorDelay: delay within [d*(1-f), d*(1+f)]."""
    policy = RetryPolicy(delay=Fixed(1.0, jitter=Jitter(factor=0.25)), max_retries=None)
    Failsafe(policy, clock=clock).call(flaky(50))
    assert all(0.75 <= slept <= 1.25 for slept in clock.slept)
    assert len(set(clock.slept)) > 1, "jitter must actually vary the delay"


def test_absolute_jitter_stays_in_range(clock: FakeClock) -> None:
    """Go TestJitteredDelay: delay within [d-j, d+j]."""
    policy = RetryPolicy(delay=Fixed(1.0, jitter=Jitter(seconds=0.2)), max_retries=None)
    Failsafe(policy, clock=clock).call(flaky(50))
    assert all(0.8 <= slept <= 1.2 for slept in clock.slept)


def test_bare_float_jitter_is_a_factor(clock: FakeClock) -> None:
    policy = RetryPolicy(delay=Fixed(1.0, jitter=0.25), max_retries=None)
    Failsafe(policy, clock=clock).call(flaky(20))
    assert all(0.75 <= slept <= 1.25 for slept in clock.slept)


def test_jitter_requires_exactly_one_form() -> None:
    with pytest.raises(ValueError, match="exactly one"):
        Jitter()
    with pytest.raises(ValueError, match="exactly one"):
        Jitter(factor=0.25, seconds=0.1)


def test_jitter_does_not_compound_into_the_backoff(clock: FakeClock) -> None:
    """Go applies jitter to the delay, never to lastDelay, so it cannot compound."""
    policy = RetryPolicy(delay=Backoff(1.0, 100.0, jitter=Jitter(factor=0.5)), max_retries=None)
    Failsafe(policy, clock=clock).call(flaky(4))
    # Each delay is within ±50% of the un-jittered 1, 2, 4, 8 sequence.
    for slept, base in zip(clock.slept, [1.0, 2.0, 4.0, 8.0], strict=True):
        assert base * 0.5 <= slept <= base * 1.5


def test_callable_delay_sees_the_execution(clock: FakeClock) -> None:
    """Go WithDelayFunc: the delay is computed per attempt from the execution."""
    policy = RetryPolicy(delay=lambda ex: float(ex.attempts), max_retries=None)
    Failsafe(policy, clock=clock).call(flaky(3))
    assert clock.slept == [1.0, 2.0, 3.0]


def test_delay_is_clamped_to_the_max_duration_budget(clock: FakeClock) -> None:
    """Go adjustForMaxDuration: delay = max(0, min(delay, max_duration - elapsed))."""
    policy = RetryPolicy(delay=100.0, max_duration=10.0, max_retries=None)
    with pytest.raises(RetriesExceeded):
        Failsafe(policy, clock=clock).call(flaky(99))
    assert clock.slept == [10.0], "a long backoff must never overshoot max_duration"


def test_max_duration_stops_retrying(clock: FakeClock) -> None:
    """Go: maxDurationExceeded is elapsed > maxDuration, checked after a failure."""
    policy = RetryPolicy(delay=3.0, max_duration=10.0, max_retries=None)
    with pytest.raises(RetriesExceeded):
        Failsafe(policy, clock=clock).call(flaky(99))
    assert sum(clock.slept) <= 10.0


def test_handle_narrows_which_exceptions_retry(clock: FakeClock) -> None:
    """Go HandleErrorTypes: only the named types are failures."""
    calls = 0

    def fn() -> str:
        nonlocal calls
        calls += 1
        raise ValueError("not handled")

    policy = RetryPolicy(handle=ConnectionError, max_retries=5)
    with pytest.raises(ValueError, match="not handled"):
        Failsafe(policy, clock=clock).call(fn)
    assert calls == 1, "an unhandled exception is not a failure and is not retried"


def test_handle_accepts_a_tuple_of_types(clock: FakeClock) -> None:
    policy = RetryPolicy(handle=(ValueError, ConnectionError), max_retries=3)
    assert Failsafe(policy, clock=clock).call(flaky(2)) == "ok"


def test_handle_matches_through_the_cause_chain(clock: FakeClock) -> None:
    """errors.Is walks Unwrap; isinstance does not, so __cause__ is walked explicitly."""

    def fn() -> str:
        try:
            raise ConnectionError("inner")
        except ConnectionError as exc:
            raise RuntimeError("outer") from exc

    policy = RetryPolicy(handle=ConnectionError, max_retries=1)
    with pytest.raises(RetriesExceeded):
        Failsafe(policy, clock=clock).call(fn)


def test_handle_does_not_match_through_implicit_context(clock: FakeClock) -> None:
    """__context__ would match exceptions raised incidentally while handling another."""
    calls = 0

    def fn() -> str:
        nonlocal calls
        calls += 1
        try:
            raise ConnectionError("incidental")
        except ConnectionError:
            raise RuntimeError("real failure")  # noqa: B904

    policy = RetryPolicy(handle=ConnectionError, max_retries=5)
    with pytest.raises(RuntimeError, match="real failure"):
        Failsafe(policy, clock=clock).call(fn)
    assert calls == 1


def test_handle_matches_an_exception_group_leaf(clock: FakeClock) -> None:
    def fn() -> str:
        raise ExceptionGroup("group", [ValueError("x"), ConnectionError("y")])

    policy = RetryPolicy(handle=ConnectionError, max_retries=1)
    with pytest.raises(RetriesExceeded):
        Failsafe(policy, clock=clock).call(fn)


def test_handle_result_retries_a_value(clock: FakeClock) -> None:
    """Go HandleResult: a returned value counts as a failure."""
    results = iter(["bad", "bad", "good"])
    policy = RetryPolicy(handle=Result("bad"), max_retries=5)
    assert Failsafe(policy, clock=clock).call(lambda: next(results)) == "good"


def test_result_condition_does_not_replace_default_error_handling(clock: FakeClock) -> None:
    """PLAN section 8 item 4, Go's errorsChecked: handle=Result(x) keeps errors failing."""
    policy = RetryPolicy(handle=Result("bad"), max_retries=2)
    with pytest.raises(RetriesExceeded):
        Failsafe(policy, clock=clock).call(flaky(9))


def test_an_error_condition_does_replace_default_error_handling(clock: FakeClock) -> None:
    """Go: once any error-aware condition exists, unmatched errors are not failures."""
    calls = 0

    def fn() -> str:
        nonlocal calls
        calls += 1
        raise ValueError("unmatched")

    policy = RetryPolicy(handle=[Result("bad"), ConnectionError], max_retries=5)
    with pytest.raises(ValueError):
        Failsafe(policy, clock=clock).call(fn)
    assert calls == 1


def test_handle_predicate_sees_result_and_exception(clock: FakeClock) -> None:
    """Go HandleIf, which also marks errors as checked."""
    seen: list[tuple[object, object]] = []

    def predicate(result: object, exc: Exception | None) -> bool:
        seen.append((result, exc))
        return result == "retry me"

    results = iter(["retry me", "done"])
    policy = RetryPolicy(handle=predicate, max_retries=3)
    assert Failsafe(policy, clock=clock).call(lambda: next(results)) == "done"
    assert seen == [("retry me", None), ("done", None)]


def test_conditions_are_ored(clock: FakeClock) -> None:
    """Go: multiple handle conditions are logically OR'ed."""
    results = iter(["a", "b", "ok"])
    policy = RetryPolicy(handle=[Result("a"), Result("b")], max_retries=5)
    assert Failsafe(policy, clock=clock).call(lambda: next(results)) == "ok"


def test_abort_stops_retrying_with_the_failure(clock: FakeClock) -> None:
    """Go AbortOnErrorTypes: the failing result is returned, not RetriesExceeded."""
    calls = 0

    def fn() -> str:
        nonlocal calls
        calls += 1
        raise PermissionError("fatal")

    policy = RetryPolicy(abort=PermissionError, max_retries=5)
    with pytest.raises(PermissionError, match="fatal"):
        Failsafe(policy, clock=clock).call(fn)
    assert calls == 1


def test_abort_only_applies_to_outcomes_that_are_failures(clock: FakeClock) -> None:
    """Go: abort is evaluated inside OnFailure, so a success never aborts."""
    policy = RetryPolicy(abort=Result("ok"), max_retries=3)
    assert Failsafe(policy, clock=clock).call(lambda: "ok") == "ok"


def test_abort_on_a_result_value(clock: FakeClock) -> None:
    results = iter(["bad", "fatal", "never"])
    policy = RetryPolicy(handle=Result("bad"), abort=Result("fatal"), max_retries=9)
    # "fatal" is not a failure under handle, so it is simply returned.
    assert Failsafe(policy, clock=clock).call(lambda: next(results)) == "fatal"


def test_cancelled_error_is_never_retried() -> None:
    """PORT.md item 7: CancelledError is terminal, not a failure."""
    calls = 0

    async def fn() -> str:
        nonlocal calls
        calls += 1
        raise asyncio.CancelledError

    async def scenario() -> None:
        with pytest.raises(asyncio.CancelledError):
            await AsyncFailsafe(RetryPolicy(max_retries=5), clock=FakeClock()).call(fn)

    asyncio.run(scenario())
    assert calls == 1


def test_execution_counters_during_retries(clock: FakeClock) -> None:
    seen: list[tuple[int, int, int, bool, bool]] = []
    fn = flaky(2)

    def record() -> str:
        ex = current_execution()
        assert ex is not None
        seen.append((ex.attempts, ex.executions, ex.retries, ex.is_first_attempt, ex.is_retry))
        return fn()

    Failsafe(RetryPolicy(max_retries=3), clock=clock).call(record)
    assert seen == [
        (1, 0, 0, True, False),
        (2, 1, 1, False, True),
        (3, 2, 2, False, True),
    ]


def test_listener_order_for_a_retried_failure(clock: FakeClock) -> None:
    """Go order: on_failure, then scheduled, then the delay, then on_retry."""
    log: list[str] = []
    policy = RetryPolicy(
        delay=1.0,
        max_retries=1,
        on_failure=lambda e: log.append("failure"),
        on_retry_scheduled=lambda e: log.append(f"scheduled {e.delay}"),
        on_retry=lambda e: log.append(f"retry at t={clock.now()}"),
        on_success=lambda e: log.append("success"),
    )
    assert Failsafe(policy, clock=clock).call(flaky(1)) == "ok"
    assert log == ["failure", "scheduled 1.0", "retry at t=1.0", "success"]


def test_retries_exceeded_listener_fires_once(clock: FakeClock) -> None:
    log: list[str] = []
    policy = RetryPolicy(
        max_retries=2,
        on_failure=lambda e: log.append("failure"),
        on_retries_exceeded=lambda e: log.append("exceeded"),
    )
    with pytest.raises(RetriesExceeded):
        Failsafe(policy, clock=clock).call(flaky(9))
    assert log == ["failure", "failure", "failure", "exceeded"]


def test_abort_returns_the_failure_when_retries_remain(clock: FakeClock) -> None:
    """Go: an abort with retries left returns the failure itself, unwrapped."""
    log: list[str] = []
    policy = RetryPolicy(
        abort=PermissionError,
        max_retries=5,
        on_abort=lambda e: log.append("abort"),
        on_retries_exceeded=lambda e: log.append("exceeded"),
    )

    def fn() -> str:
        raise PermissionError

    with pytest.raises(PermissionError):
        Failsafe(policy, clock=clock).call(fn)
    assert log == ["abort"]


def test_exceeded_wins_over_abort_on_the_last_attempt(clock: FakeClock) -> None:
    """Go OnFailure: on_abort fires, on_retries_exceeded is suppressed, but the
    returned result is still ExceededError unless return_last_failure."""
    log: list[str] = []
    policy = RetryPolicy(
        abort=PermissionError,
        max_retries=0,
        on_abort=lambda e: log.append("abort"),
        on_retries_exceeded=lambda e: log.append("exceeded"),
    )

    def fn() -> str:
        raise PermissionError

    with pytest.raises(RetriesExceeded) as info:
        Failsafe(policy, clock=clock).call(fn)
    assert isinstance(info.value.last_exception, PermissionError)
    assert log == ["abort"], "on_retries_exceeded is suppressed by an abort"


def test_listeners_accept_a_sequence(clock: FakeClock) -> None:
    log: list[str] = []
    policy = RetryPolicy(
        max_retries=1,
        on_failure=[lambda e: log.append("a"), lambda e: log.append("b")],
    )
    assert Failsafe(policy, clock=clock).call(flaky(1)) == "ok"
    assert log == ["a", "b"]


def test_listener_registration_doubles_as_a_decorator(clock: FakeClock) -> None:
    """D-10: policy.on_retry(fn) appends and returns fn."""
    policy = RetryPolicy(max_retries=1)
    log: list[int] = []

    @policy.on_retry.add
    def record(event: object) -> None:
        log.append(1)

    assert callable(record), "the decorator must return the function"
    Failsafe(policy, clock=clock).call(flaky(1))
    assert log == [1]


def test_listener_exception_does_not_break_the_execution(clock: FakeClock) -> None:
    """PORT.md item 5: a metrics bug must not break a request."""

    def boom(event: object) -> None:
        raise RuntimeError("metrics is down")

    policy = RetryPolicy(max_retries=2, on_failure=boom, on_retry=boom, on_success=boom)
    assert Failsafe(policy, clock=clock).call(flaky(1)) == "ok"


def test_listener_event_carries_the_failure(clock: FakeClock) -> None:
    seen: list[tuple[int, object]] = []
    policy = RetryPolicy(
        max_retries=2,
        on_failure=lambda e: seen.append((e.attempts, e.exception)),
    )
    Failsafe(policy, clock=clock).call(flaky(1))
    assert seen[0][0] == 1
    assert isinstance(seen[0][1], ConnectionError)


def test_shared_policy_drives_both_drivers(clock: FakeClock) -> None:
    policy = RetryPolicy(delay=1.0, max_retries=3)
    async_clock = FakeClock()

    assert Failsafe(policy, clock=clock).call(flaky(2)) == "ok"

    async def scenario() -> str:
        result = await AsyncFailsafe(policy, clock=async_clock).call(aflaky(2))
        assert isinstance(result, str)
        return result

    assert asyncio.run(scenario()) == "ok"
    assert clock.slept == async_clock.slept == [1.0, 1.0]
