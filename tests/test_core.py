"""Tests for the sans-io core: composition, outcomes, Execution, the clock seam."""

from __future__ import annotations

import pytest
from helpers import Retry, Tracer, flaky

from failsafe import Execution, Failsafe, current_execution
from failsafe._core import FakeClock, Outcome, compose


def test_outcome_unwrap_raises_its_exception() -> None:
    exc = ConnectionError("boom")
    with pytest.raises(ConnectionError) as info:
        Outcome(exc=exc).unwrap()
    assert info.value is exc


def test_composition_order_is_outermost_first() -> None:
    log: list[str] = []
    fs = Failsafe(Tracer("outer", log), Tracer("middle", log), Tracer("inner", log))
    assert fs.call(lambda: "ok") == "ok"
    assert log == [
        "enter outer",
        "enter middle",
        "enter inner",
        "exit inner",
        "exit middle",
        "exit outer",
    ]


def test_chain_is_composed_once_not_per_call() -> None:
    """Re-composing per call costs more than the rest of the core (PLAN D-7)."""
    applied = 0

    class CountingPolicy:
        def apply(self, inner):  # type: ignore[no-untyped-def]
            nonlocal applied
            applied += 1
            return inner

    fs = Failsafe(CountingPolicy())
    assert applied == 1
    for _ in range(5):
        fs.call(lambda: "ok")
    assert applied == 1


def test_execution_has_slots_and_no_dict() -> None:
    ex = Execution(FakeClock())
    assert not hasattr(ex, "__dict__")
    with pytest.raises(AttributeError):
        ex.unexpected = 1  # type: ignore[attr-defined]


def test_retry_counts_attempts_executions_and_retries() -> None:
    clock = FakeClock()
    seen: list[tuple[int, int, int]] = []

    def record() -> str:
        ex = current_execution()
        assert ex is not None
        seen.append((ex.attempts, ex.executions, ex.retries))
        return flaky_fn()

    flaky_fn = flaky(2)
    fs = Failsafe(Retry(max_retries=3), clock=clock)
    assert fs.call(record) == "ok"
    assert seen == [(1, 0, 0), (2, 1, 1), (3, 2, 2)]


def test_execution_records_last_result_and_exception() -> None:
    clock = FakeClock()
    lasts: list[tuple[object, object]] = []

    def record() -> str:
        ex = current_execution()
        assert ex is not None
        lasts.append((ex.last_result, ex.last_exception))
        return flaky_fn()

    flaky_fn = flaky(1)
    Failsafe(Retry(max_retries=1), clock=clock).call(record)
    assert lasts[0] == (None, None)
    assert lasts[1][0] is None
    assert isinstance(lasts[1][1], ConnectionError)


def test_sleep_goes_through_the_clock_seam() -> None:
    clock = FakeClock()
    fs = Failsafe(Retry(max_retries=3, delay=2.5), clock=clock)
    assert fs.call(flaky(2)) == "ok"
    assert clock.slept == [2.5, 2.5]
    assert clock.now() == 5.0


def test_elapsed_tracks_the_injected_clock() -> None:
    clock = FakeClock()
    ex = Execution(clock)
    clock.advance(7.0)
    assert ex.elapsed == 7.0


def test_remaining_is_none_without_a_deadline_and_clamps_at_zero() -> None:
    clock = FakeClock()
    assert Execution(clock).remaining() is None
    ex = Execution(clock, deadline=clock.now() + 5.0)
    clock.advance(2.0)
    assert ex.remaining() == 3.0
    clock.advance(10.0)
    assert ex.remaining() == 0.0


def test_attempt_start_time_resets_per_attempt() -> None:
    clock = FakeClock()
    elapsed_attempt: list[float] = []

    def record() -> str:
        ex = current_execution()
        assert ex is not None
        elapsed_attempt.append(ex.elapsed_attempt)
        clock.advance(1.0)
        return flaky_fn()

    flaky_fn = flaky(1)
    Failsafe(Retry(max_retries=1, delay=4.0), clock=clock).call(record)
    assert elapsed_attempt == [0.0, 0.0]


def test_is_first_attempt_and_is_retry() -> None:
    clock = FakeClock()
    flags: list[tuple[bool, bool]] = []

    def record() -> str:
        ex = current_execution()
        assert ex is not None
        flags.append((ex.is_first_attempt, ex.is_retry))
        return flaky_fn()

    flaky_fn = flaky(1)
    Failsafe(Retry(max_retries=1), clock=clock).call(record)
    assert flags == [(True, False), (False, True)]


def test_base_exception_is_never_caught_or_retried() -> None:
    """A cancelled or interrupted caller is not a retryable failure (PLAN invariants)."""
    calls = 0

    def interrupted() -> str:
        nonlocal calls
        calls += 1
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        Failsafe(Retry(max_retries=5), clock=FakeClock()).call(interrupted)
    assert calls == 1


def test_exhausted_retries_raise_the_last_exception() -> None:
    with pytest.raises(ConnectionError, match="boom 3"):
        Failsafe(Retry(max_retries=2), clock=FakeClock()).call(flaky(9))


def test_current_execution_is_none_outside_a_call() -> None:
    assert current_execution() is None
    Failsafe().call(lambda: None)
    assert current_execution() is None


def test_compose_with_no_policies_invokes_once() -> None:
    ex = Execution(FakeClock())
    gen = compose(())(ex)
    gen.send(None)
    with pytest.raises(StopIteration) as info:
        gen.send(Outcome(value=42))
    assert info.value.value.unwrap() == 42
    assert (ex.attempts, ex.executions) == (1, 1)
