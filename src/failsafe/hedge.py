"""Concurrent backup attempts for slow executions, ported from failsafe-go's ``hedgepolicy``."""

from __future__ import annotations

import threading
from collections.abc import Callable, Generator
from datetime import timedelta
from typing import Any

from ._conditions import UNSET, Condition, FailureConditions
from ._core import Execution, Hedge, Instruction, Outcome, Step
from ._delays import seconds
from ._events import ExecutionEvent, Listener, Listeners
from ._windows import MovingQuantile
from .budget import Budget

__all__ = ["HedgePolicy"]

_QUANTILE_STEP = 0.01


class HedgePolicy:
    """Starts additional attempts when an execution is slow, taking the first result.

    The tail of a latency distribution is usually not the service being slow but one
    request being unlucky: a stalled connection, an unlucky GC pause, a node about to
    be replaced. Retrying only after a timeout pays the timeout first. A hedge starts
    a second attempt while the first is still running and takes whichever finishes.

    Unlike [`RetryPolicy`][failsafe.RetryPolicy], attempts overlap. The losers are cancelled
    for real once a winner is accepted::

        api = AsyncFailsafe(HedgePolicy(0.1))          # hedge after 100ms
        api = AsyncFailsafe(HedgePolicy(0.1, max_hedges=2))

    **Async only.** Under [`Failsafe`][failsafe.Failsafe] this raises
    `NotImplementedError`: a synchronous call cannot be cancelled, so the losing
    attempts would run to completion against the service this policy is trying to
    protect. The capability gap is deliberate rather than papered over with threads.

    **Hedges multiply load.** Every attempt is a real request, so a policy that hedges
    eagerly turns a slow dependency into an overloaded one. Hedge on a delay well into
    the tail, and give the policy a [`Budget`][failsafe.Budget] when the dependency is
    shared.

    Args:
        delay: How long to wait before starting another attempt. Seconds, a
            timedelta, or a callable taking the [`Execution`][failsafe.Execution] and
            returning seconds, or ``None`` to stop hedging. Exactly one of ``delay``
            or ``quantile`` is required.
        quantile: Hedge when an execution exceeds this quantile of recent accepted
            durations, between 0 and 1 exclusive. Adapts to a service whose latency
            changes, where a fixed delay would need retuning.
        quantile_age: How many executions the quantile estimate remembers. Smaller
            ages adapt faster; larger ages are steadier.
        warmup: How many executions to observe before a ``quantile`` policy starts
            hedging. Hedging on an estimate drawn from a handful of samples is how a
            cold start becomes a thundering herd.
        max_hedges: How many extra attempts may be started beyond the first.
        cancel_on: Which outcomes end the execution and cancel the attempts still
            running. Defaults to the first outcome to arrive, whatever it is.
        budget: A [`Budget`][failsafe.Budget] limiting what share of in-flight work
            hedges may become. When it is spent the policy stops hedging, and the
            attempts already running still produce a result.
        on_hedge: Called before each hedged attempt starts.

    Raises:
        ValueError: If neither or both of ``delay`` and ``quantile`` are given, if
            ``quantile`` is not between 0 and 1 exclusive, if ``quantile_age`` or
            ``warmup`` is not positive, if ``max_hedges`` is not positive, or if a
            fixed ``delay`` is negative.

    Attributes:
        on_hedge: Listeners called before each hedged attempt.
    """

    __slots__ = (
        "_accept",
        "_delay",
        "_lock",
        "_quantile",
        "_warmup",
        "budget",
        "max_hedges",
        "on_hedge",
    )

    def __init__(
        self,
        delay: float | timedelta | Callable[[Execution], float | None] | None = None,
        *,
        quantile: float | None = None,
        quantile_age: int = 30,
        warmup: int = 20,
        max_hedges: int = 1,
        cancel_on: Condition = UNSET,
        budget: Budget | None = None,
        on_hedge: Listener | None = None,
    ) -> None:
        if (delay is None) == (quantile is None):
            raise ValueError("exactly one of delay or quantile is required")
        if max_hedges <= 0:
            raise ValueError("max_hedges must be positive")
        if quantile is not None:
            if not 0 < quantile < 1:
                raise ValueError("quantile must be between 0 and 1 exclusive")
            if quantile_age <= 0:
                raise ValueError("quantile_age must be positive")
            if warmup <= 0:
                raise ValueError("warmup must be positive")

        self._delay = _compile_delay(delay)
        self._quantile = (
            None if quantile is None else MovingQuantile(quantile, _QUANTILE_STEP, quantile_age)
        )
        self._warmup = warmup
        self._accept = FailureConditions(cancel_on) if cancel_on is not UNSET else None
        self.max_hedges = max_hedges
        """How many extra attempts may be started."""
        self.budget = budget
        """The shared hedge budget, if any."""
        self.on_hedge = Listeners("on_hedge", on_hedge)
        """Listeners called before each hedged attempt."""
        self._lock = threading.Lock()

    def apply(self, inner: Step) -> Step:
        """Wrap ``inner`` so slow attempts are joined by concurrent ones."""

        def step(ex: Execution) -> Generator[Instruction, Any, Outcome[Any]]:
            control = _Control(self, ex)
            if self.budget is not None:
                self.budget.record_execution()
            try:
                outcome: Outcome[Any] = yield Hedge(inner, control)
            finally:
                if self.budget is not None:
                    self.budget.release_execution()
            return outcome

        return step

    def _accepts(self, outcome: Outcome[Any]) -> bool:
        """Whether this outcome should win and cancel the attempts still running."""
        if self._accept is None:
            return True
        return self._accept(outcome.value, outcome.exc)

    def _hedge_delay(self, ex: Execution) -> float | None:
        """Seconds before another attempt starts, or ``None`` to stop hedging."""
        if self._quantile is None:
            return self._delay(ex)
        with self._lock:
            if self._quantile.count < self._warmup:
                return None
            return self._quantile.value

    def _record(self, duration: float) -> None:
        """Feed an accepted execution's duration to the quantile estimate."""
        if self._quantile is not None:
            with self._lock:
                self._quantile.add(duration)


class _Control:
    """The per-execution state a driver needs to run one hedged execution."""

    __slots__ = ("_ex", "_policy", "_started")

    def __init__(self, policy: HedgePolicy, ex: Execution) -> None:
        self._policy = policy
        self._ex = ex
        self._started = 0

    def hedge_delay(self, ex: Execution) -> float | None:
        if self._started >= self._policy.max_hedges:
            return None
        return self._policy._hedge_delay(ex)

    def accepts(self, outcome: Outcome[Any]) -> bool:
        return self._policy._accepts(outcome)

    def start_hedge(self, ex: Execution) -> bool:
        budget = self._policy.budget
        if budget is not None and not budget.try_acquire_permit():
            budget.notify_exceeded(ex, "hedge")
            return False
        self._started += 1
        if self._policy.on_hedge:
            self._policy.on_hedge.emit(ExecutionEvent(ex, None, None))
        return True

    def finish_hedge(self) -> None:
        if self._policy.budget is not None:
            self._policy.budget.release_permit()

    def record_duration(self, ex: Execution) -> None:
        """Record how long one attempt took, for a quantile-based delay."""
        self._policy._record(ex.elapsed_attempt)


def _compile_delay(
    delay: float | timedelta | Callable[[Execution], float | None] | None,
) -> Callable[[Execution], float | None]:
    """Compile the configured delay into a function of the execution."""
    if delay is None:
        return lambda _ex: None
    if isinstance(delay, (int, float, timedelta)):
        fixed = seconds(delay)
        if fixed < 0:
            raise ValueError("delay must not be negative")
        return lambda _ex: fixed
    return delay
