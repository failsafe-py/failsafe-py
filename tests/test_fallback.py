"""Fallback semantics, ported from failsafe-go's `fallback` package."""

from __future__ import annotations

import pytest
from conftest import flaky

from failsafe import (
    Execution,
    Failsafe,
    Fallback,
    RetriesExceeded,
    RetryPolicy,
    current_execution,
)
from failsafe._core import FakeClock


def test_value_replaces_a_failure(clock: FakeClock) -> None:
    """Go NewWithResult."""
    assert Failsafe(Fallback(value="cached"), clock=clock).call(flaky(9)) == "cached"


def test_success_passes_through_untouched(clock: FakeClock) -> None:
    """Go: the fallback does not fire when the result is not a failure."""
    assert Failsafe(Fallback(value="cached"), clock=clock).call(lambda: "live") == "live"


def test_fn_sees_the_failure_through_the_execution(clock: FakeClock) -> None:
    """Go: the fn receives CopyWithResult, so last_exception is the failure handled."""
    seen: list[object] = []

    def recover(ex: Execution) -> str:
        seen.append(ex.last_exception)
        return "recovered"

    assert Failsafe(Fallback(fn=recover), clock=clock).call(flaky(9)) == "recovered"
    assert isinstance(seen[0], ConnectionError)


def test_fn_exception_replaces_the_original(clock: FakeClock) -> None:
    """Go: the fallback's own error becomes the result, unwrapped."""

    def failing(ex: Execution) -> str:
        raise RuntimeError("fallback also failed")

    with pytest.raises(RuntimeError, match="fallback also failed"):
        Failsafe(Fallback(fn=failing), clock=clock).call(flaky(9))


def test_handle_narrows_what_the_fallback_catches(clock: FakeClock) -> None:
    """Go HandleErrorTypes: an unmatched error is a success, so no fallback."""
    policy = Fallback(value="cached", handle=ConnectionError)

    def fn() -> str:
        raise ValueError("unmatched")

    with pytest.raises(ValueError, match="unmatched"):
        Failsafe(policy, clock=clock).call(fn)


def test_handle_result_fires_on_a_value(clock: FakeClock) -> None:
    policy = Fallback(value="cached", handle=None)
    assert Failsafe(policy, clock=clock).call(lambda: None) == "cached"


def test_outermost_fallback_catches_retries_exceeded(clock: FakeClock) -> None:
    """The documented composition: Fallback(RetryPolicy(fn))."""
    fs = Failsafe(Fallback(value="cached"), RetryPolicy(max_retries=1), clock=clock)
    assert fs.call(flaky(9)) == "cached"


def test_narrowed_fallback_matches_through_the_wrapper(clock: FakeClock) -> None:
    """Go's ExceededError.Unwrap exposes LastError, so errors.As finds the cause.

    Verified against Go: a Fallback narrowed to the *inner* error type still catches a
    retries-exceeded wrapper, because the wrapper unwraps to the failure that caused
    it. The __cause__ walk reproduces this.
    """
    fs = Failsafe(
        Fallback(value="cached", handle=ConnectionError),
        RetryPolicy(max_retries=1),
        clock=clock,
    )
    assert fs.call(flaky(9)) == "cached"

    fs_wrapper = Failsafe(
        Fallback(value="cached", handle=RetriesExceeded),
        RetryPolicy(max_retries=1),
        clock=clock,
    )
    assert fs_wrapper.call(flaky(9)) == "cached"

    fs_unrelated = Failsafe(
        Fallback(value="cached", handle=PermissionError),
        RetryPolicy(max_retries=1),
        clock=clock,
    )
    with pytest.raises(RetriesExceeded):
        fs_unrelated.call(flaky(9))


def test_listeners_report_the_fallback_result(clock: FakeClock) -> None:
    log: list[str] = []
    policy = Fallback(
        value="cached",
        on_success=lambda e: log.append("success"),
        on_failure=lambda e: log.append(f"failure {type(e.exception).__name__}"),
        on_fallback=lambda e: log.append(f"fallback {e.result}"),
    )
    assert Failsafe(policy, clock=clock).call(flaky(9)) == "cached"
    assert log == ["failure ConnectionError", "fallback cached"]

    log.clear()
    assert Failsafe(policy, clock=clock).call(lambda: "live") == "live"
    assert log == ["success"]


def test_requires_exactly_one_of_value_or_fn() -> None:
    with pytest.raises(ValueError, match="exactly one"):
        Fallback()
    with pytest.raises(ValueError, match="exactly one"):
        Fallback(value="x", fn=lambda ex: "y")


def test_value_none_is_a_valid_fallback(clock: FakeClock) -> None:
    """None must be distinguishable from 'no value given'."""
    assert Failsafe(Fallback(value=None), clock=clock).call(flaky(9)) is None


def test_fn_reads_the_current_execution(clock: FakeClock) -> None:
    attempts: list[int] = []
    fs = Failsafe(
        Fallback(fn=lambda ex: f"after {ex.attempts}"),
        RetryPolicy(max_retries=2),
        clock=clock,
    )
    assert fs.call(flaky(9)) == "after 3"
    assert current_execution() is None
    assert attempts == []
