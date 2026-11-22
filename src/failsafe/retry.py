"""Retries failed executions, optionally with a delay between attempts."""

from __future__ import annotations

import random
from collections.abc import Generator
from datetime import timedelta
from typing import Any

from ._conditions import UNSET, Condition, FailureConditions
from ._core import Execution, Instruction, Outcome, Sleep, Step
from ._delays import Delay, compile_delay, seconds
from ._events import ExecutionEvent, Listener, Listeners, ScheduledRetryEvent
from ._exceptions import BudgetExceeded, RetriesExceeded
from .budget import Budget

__all__ = ["RetryPolicy"]


class RetryPolicy:
    """Retries an execution until it succeeds or the configured limits are reached.

    By default three attempts are made with no delay, and any exception is a failure.

    Stateless and safe to share across executions and across both drivers: every piece
    of per-execution state, including the backoff's current delay, lives in the
    generator rather than on the policy.

    Example:
        >>> retry = RetryPolicy(
        ...     handle=ConnectionError,
        ...     delay=Backoff(1.0, 30.0, jitter=Jitter(factor=0.25)),
        ...     max_retries=3,
        ... )
        >>> Failsafe(retry).call(connect, host)

    Args:
        handle: What counts as a failure worth retrying. Defaults to any exception.
            A result-only condition such as ``handle=None`` adds to the default rather
            than replacing it, matching Go.
        abort: What should stop retrying immediately, failing with that result rather
            than [`RetriesExceeded`][failsafe.RetriesExceeded]. Only evaluated for outcomes that
            ``handle`` already considers failures.
        delay: How long to wait between attempts. Seconds, a timedelta,
            [`Fixed`][failsafe.Fixed], [`Backoff`][failsafe.Backoff],
            [`RandomDelay`][failsafe.RandomDelay], or a callable taking the execution.
        max_retries: How many retries to allow after the first attempt, so the default
            of 2 permits 3 attempts. ``None`` allows unlimited retries, bounded only by
            ``max_duration``.
        max_duration: Give up once this many seconds have elapsed since the execution
            began. Delays are clamped so a long backoff never overshoots it.
        return_last_failure: Raise or return the last failure itself rather than
            wrapping it in [`RetriesExceeded`][failsafe.RetriesExceeded].
        budget: A [`Budget`][failsafe.Budget] limiting what share of in-flight work
            retries may become. Share one instance across the policies guarding the
            same dependency. Retries refused by the budget fail with
            [`BudgetExceeded`][failsafe.BudgetExceeded].
        on_retry: Called after each delay, before the next attempt.
        on_retry_scheduled: Called before each delay, with the delay about to elapse.
        on_retries_exceeded: Called once when the limits are reached.
        on_abort: Called once when an ``abort`` condition matches.
        on_success: Called when an attempt is not a failure.
        on_failure: Called after every failed attempt, including retried ones.
        rng: Custom source of jitter and random-delay randomness (e.g. for testing).

    Raises:
        ValueError: If ``max_retries`` or ``max_duration`` is negative.
    """

    __slots__ = (
        "_abort",
        "_delay",
        "_handle",
        "_rng",
        "budget",
        "max_duration",
        "max_retries",
        "on_abort",
        "on_failure",
        "on_retries_exceeded",
        "on_retry",
        "on_retry_scheduled",
        "on_success",
        "return_last_failure",
    )

    def __init__(
        self,
        *,
        handle: Condition = UNSET,
        abort: Condition = UNSET,
        delay: Delay | None = None,
        max_retries: int | None = 2,
        max_duration: float | timedelta | None = None,
        return_last_failure: bool = False,
        budget: Budget | None = None,
        on_retry: Listener | None = None,
        on_retry_scheduled: Listener | None = None,
        on_retries_exceeded: Listener | None = None,
        on_abort: Listener | None = None,
        on_success: Listener | None = None,
        on_failure: Listener | None = None,
        rng: random.Random | None = None,
    ) -> None:
        if max_retries is not None and max_retries < 0:
            raise ValueError("max_retries must not be negative; use None for unlimited")
        if max_duration is not None and seconds(max_duration) < 0:
            raise ValueError("max_duration must not be negative")
        self._handle = FailureConditions(handle)
        self._abort = FailureConditions(abort) if abort is not UNSET else None
        self._delay = delay
        self._rng = rng if rng is not None else random.Random()
        self.max_retries = max_retries
        """How many retries are allowed after the first attempt."""
        self.max_duration = None if max_duration is None else seconds(max_duration)
        """Seconds after which no further retry is started."""
        self.return_last_failure = return_last_failure
        """Whether the last failure is returned rather than wrapped."""
        self.budget = budget
        """The shared retry budget, if any."""
        self.on_retry = Listeners("on_retry", on_retry)
        """Listeners called after each delay, before the next attempt."""
        self.on_retry_scheduled = Listeners("on_retry_scheduled", on_retry_scheduled)
        """Listeners called before each delay."""
        self.on_retries_exceeded = Listeners("on_retries_exceeded", on_retries_exceeded)
        """Listeners called once when the retry limits are reached."""
        self.on_abort = Listeners("on_abort", on_abort)
        """Listeners called once when an abort condition matches."""
        self.on_success = Listeners("on_success", on_success)
        """Listeners called when an attempt is not a failure."""
        self.on_failure = Listeners("on_failure", on_failure)
        """Listeners called after every failed attempt."""

    def is_failure(self, outcome: Outcome[Any]) -> bool:
        """Whether ``outcome`` is a failure this policy would retry."""
        return self._handle(outcome.value, outcome.exc)

    def apply(self, inner: Step) -> Step:
        """Wrap ``inner`` in the retry loop. Called once, at composition time."""

        def step(ex: Execution) -> Generator[Instruction, Any, Outcome[Any]]:
            # Per-execution state, deliberately local: the policy object is shared.
            next_delay = compile_delay(self._delay, self._rng)
            failures = 0
            budget = self.budget
            holding = False
            if budget is not None:
                budget.record_execution()
            try:
                while True:
                    outcome = yield from inner(ex)
                    if budget is not None and holding:
                        budget.release_permit()
                        holding = False

                    if not self.is_failure(outcome):
                        if self.on_success:
                            self.on_success.emit(_event(ex, outcome))
                        return outcome

                    if self.on_failure:
                        self.on_failure.emit(_event(ex, outcome))

                    failures += 1
                    # Go compares elapsed > max_duration, but clamping the delay lands
                    # elapsed exactly on the budget, so a strict compare never trips on a
                    # deterministic clock and the loop spins. >= is the same bound.
                    exceeded = (self.max_retries is not None and failures > self.max_retries) or (
                        self.max_duration is not None and ex.elapsed >= self.max_duration
                    )
                    aborted = self._abort is not None and self._abort(outcome.value, outcome.exc)

                    # Both listeners may fire, but an exceeded policy still wraps, matching
                    # Go's OnFailure: abort decides whether to stop, not what is returned.
                    if aborted and self.on_abort:
                        self.on_abort.emit(_event(ex, outcome))
                    if exceeded and not aborted and self.on_retries_exceeded:
                        self.on_retries_exceeded.emit(_event(ex, outcome))

                    if exceeded:
                        if self.return_last_failure:
                            return outcome
                        return Outcome(exc=_exceeded(outcome))
                    if aborted:
                        return outcome

                    delay = self._clamp(next_delay(ex), ex)
                    if self.on_retry_scheduled:
                        self.on_retry_scheduled.emit(
                            ScheduledRetryEvent(ex, outcome.value, outcome.exc, delay)
                        )
                    if delay > 0:
                        yield Sleep(delay)

                    # Checked after the delay, as Go does: a budget that frees up while
                    # this execution waits still permits the retry.
                    if budget is not None:
                        if not budget.try_acquire_permit():
                            budget.notify_exceeded(ex, "retry")
                            return Outcome(exc=_budget_exceeded(budget, outcome))
                        holding = True

                    if self.on_retry:
                        self.on_retry.emit(_event(ex, outcome))
            finally:
                if budget is not None:
                    if holding:
                        budget.release_permit()
                    budget.release_execution()

        return step

    def _clamp(self, delay: float, ex: Execution) -> float:
        """Clamp a delay to the remaining max-duration budget."""
        if self.max_duration is not None:
            delay = min(delay, self.max_duration - ex.elapsed)
        return max(0.0, delay)


def _event(ex: Execution, outcome: Outcome[Any]) -> ExecutionEvent:
    return ExecutionEvent(ex, outcome.value, outcome.exc)


def _exceeded(outcome: Outcome[Any]) -> RetriesExceeded:
    exceeded = RetriesExceeded(outcome.value, outcome.exc)
    # Chain the real cause so the traceback survives, as `raise ... from` would.
    exceeded.__cause__ = outcome.exc
    return exceeded


def _budget_exceeded(budget: Budget, outcome: Outcome[Any]) -> BudgetExceeded:
    exceeded = BudgetExceeded(budget.rate, budget.max_rate)
    exceeded.__cause__ = outcome.exc
    return exceeded
