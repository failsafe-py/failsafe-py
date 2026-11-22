"""Temporarily blocks executions against a failing resource, so it can recover."""

from __future__ import annotations

import random
import threading
from collections.abc import Generator
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from ._conditions import UNSET, Condition, FailureConditions
from ._core import SYSTEM_CLOCK, Clock, Execution, Instruction, Outcome, Step
from ._delays import Delay, compile_delay, seconds
from ._events import ExecutionEvent, Listener, Listeners, StateChangedEvent
from ._exceptions import CircuitBreakerOpen
from ._stats import CountingStats, Stats, TimedStats

_BUCKETS = 10
"""Go's circuitbreaker defaultBucketCount."""

__all__ = ["CircuitBreaker", "Rate", "Ratio"]

CLOSED = "closed"
"""Executions are allowed and failures are counted."""
OPEN = "open"
"""Executions are rejected until the delay elapses."""
HALF_OPEN = "half-open"
"""A bounded number of trial executions is admitted."""


@dataclass(frozen=True, slots=True)
class Ratio:
    """A threshold of ``count`` out of the last ``capacity`` executions.

    Example:
        >>> CircuitBreaker(failure_threshold=Ratio(3, 5))   # 3 of the last 5

    Args:
        count: How many must match.
        capacity: How many recent executions are considered.

    Raises:
        ValueError: If ``count`` is not positive or exceeds ``capacity``.
    """

    count: int
    capacity: int

    def __post_init__(self) -> None:
        if self.count <= 0:
            raise ValueError("ratio count must be positive")
        if self.count > self.capacity:
            raise ValueError("ratio count must not exceed capacity")


@dataclass(frozen=True, slots=True)
class Rate:
    """A threshold on the proportion of failures within a time period.

    Example:
        >>> CircuitBreaker(failure_threshold=Rate(0.5, min_executions=20, period=60.0))

    Args:
        rate: The failing proportion that trips the breaker, between 0 and 1.
        min_executions: How many executions must be recorded in the period before the
            rate is evaluated at all, so a single early failure cannot trip it.
        period: The window, in seconds or as a timedelta.

    Raises:
        ValueError: If ``rate`` is outside (0, 1], or the other values are not positive.
    """

    rate: float
    min_executions: int
    period: float | timedelta

    def __post_init__(self) -> None:
        if not 0 < self.rate <= 1:
            raise ValueError("rate must be between 0 and 1")
        if self.min_executions <= 0:
            raise ValueError("min_executions must be positive")
        if seconds(self.period) <= 0:
            raise ValueError("period must be positive")


class CircuitBreaker:
    """Blocks executions against a resource that is failing, letting it recover.

    The breaker is **closed** while the resource is healthy. Enough failures **open**
    it, and every execution is then rejected with
        [`CircuitBreakerOpen`][failsafe.CircuitBreakerOpen]
    without the function being called. After ``delay`` the breaker becomes
    **half-open**, admitting a bounded number of trial executions: enough successes
    close it, and a failure opens it again.

    The half-open cap is not decoration. Without it every caller that queued up during
    the delay is admitted at once and stampedes a service that is still recovering.

    Stateful: construct one per backend and share it. Its state is guarded by a lock
    held for microseconds, which is what lets one instance serve sync and async callers
    at the same time.

    Example:
        >>> breaker = CircuitBreaker(
        ...     failure_threshold=Rate(0.5, min_executions=20, period=60.0),
        ...     success_threshold=3,
        ...     delay=Fixed(60.0, jitter=Jitter(factor=0.2)),
        ... )

    Args:
        failure_threshold: What opens the breaker. An ``int`` means that many
            consecutive failures; a [`Ratio`][failsafe.Ratio] means that many out of the last N;
            a [`Rate`][failsafe.Rate] means that failing proportion over a period.
        success_threshold: How many trial executions must succeed to close the breaker
            again, as an ``int`` or [`Ratio`][failsafe.Ratio]. Defaults to the failure threshold,
            matching Go.
        delay: How long to stay open before admitting trials. Accepts the same forms as
            a retry delay, including a callable over the execution.
        handle: What counts as a failure. Defaults to any exception.
        clock: Custom time source, shared with the time-based window (e.g. for testing).
        on_open: Called when the breaker opens.
        on_half_open: Called when the breaker becomes half-open.
        on_close: Called when the breaker closes.
        on_state_change: Called after any of the above, with the same event.
        on_success: Called when an execution is recorded as a success.
        on_failure: Called when an execution is recorded as a failure.
        rng: Custom source of delay jitter (e.g. for testing).
    """

    __slots__ = (
        "_capacity",
        "_clock",
        "_delay",
        "_failure_rate",
        "_failure_threshold",
        "_handle",
        "_lock",
        "_min_executions",
        "_opened_at",
        "_period",
        "_permits",
        "_rng",
        "_state",
        "_stats",
        "_success_capacity",
        "_success_threshold",
        "delay_seconds",
        "on_close",
        "on_failure",
        "on_half_open",
        "on_open",
        "on_state_change",
        "on_success",
    )

    def __init__(
        self,
        *,
        failure_threshold: int | Ratio | Rate = 1,
        success_threshold: int | Ratio | None = None,
        delay: Delay | None = 60.0,
        handle: Condition = UNSET,
        clock: Clock = SYSTEM_CLOCK,
        on_open: Listener | None = None,
        on_half_open: Listener | None = None,
        on_close: Listener | None = None,
        on_state_change: Listener | None = None,
        on_success: Listener | None = None,
        on_failure: Listener | None = None,
        rng: random.Random | None = None,
    ) -> None:
        self._lock = threading.Lock()
        self._clock = clock
        self._handle = FailureConditions(handle)
        self._rng = rng if rng is not None else random.Random()
        self._delay = compile_delay(delay, self._rng)
        self.delay_seconds = 0.0
        """Seconds the breaker will stay open, as computed when it last opened."""

        self._failure_rate = 0.0
        self._min_executions = 0
        self._period: float | None = None
        if isinstance(failure_threshold, Rate):
            self._failure_rate = failure_threshold.rate
            self._min_executions = failure_threshold.min_executions
            self._period = seconds(failure_threshold.period)
            self._failure_threshold = failure_threshold.min_executions
            self._capacity = failure_threshold.min_executions
        elif isinstance(failure_threshold, Ratio):
            self._failure_threshold = failure_threshold.count
            self._capacity = failure_threshold.capacity
        else:
            if failure_threshold <= 0:
                raise ValueError("failure_threshold must be positive")
            self._failure_threshold = failure_threshold
            self._capacity = failure_threshold

        if success_threshold is None:
            self._success_threshold = 0
            self._success_capacity = 0
        elif isinstance(success_threshold, Ratio):
            self._success_threshold = success_threshold.count
            self._success_capacity = success_threshold.capacity
        else:
            if success_threshold <= 0:
                raise ValueError("success_threshold must be positive")
            self._success_threshold = success_threshold
            self._success_capacity = success_threshold

        self.on_open = Listeners("on_open", on_open)
        """Listeners called when the breaker opens."""
        self.on_half_open = Listeners("on_half_open", on_half_open)
        """Listeners called when the breaker becomes half-open."""
        self.on_close = Listeners("on_close", on_close)
        """Listeners called when the breaker closes."""
        self.on_state_change = Listeners("on_state_change", on_state_change)
        """Listeners called on any state change."""
        self.on_success = Listeners("on_success", on_success)
        """Listeners called when a success is recorded."""
        self.on_failure = Listeners("on_failure", on_failure)
        """Listeners called when a failure is recorded."""

        self._state = CLOSED
        self._opened_at = 0.0
        self._permits = 0
        self._stats: Stats = self._new_stats()

    @property
    def state(self) -> str:
        """The current state: ``"closed"``, ``"open"`` or ``"half-open"``.

        Reading never advances an expired open breaker to half-open; only an execution
        attempt does, matching Go.
        """
        with self._lock:
            return self._state

    @property
    def is_open(self) -> bool:
        """Whether the breaker is open."""
        return self.state == OPEN

    @property
    def is_closed(self) -> bool:
        """Whether the breaker is closed."""
        return self.state == CLOSED

    @property
    def is_half_open(self) -> bool:
        """Whether the breaker is half-open."""
        return self.state == HALF_OPEN

    @property
    def failures(self) -> int:
        """Failures within the current window."""
        with self._lock:
            return self._stats.failures

    @property
    def successes(self) -> int:
        """Successes within the current window."""
        with self._lock:
            return self._stats.successes

    @property
    def executions(self) -> int:
        """Executions recorded within the current window."""
        with self._lock:
            return self._stats.executions

    def failure_rate(self) -> float:
        """The failing proportion of the current window, rounded to two decimals."""
        with self._lock:
            return self._stats.failure_rate()

    def success_rate(self) -> float:
        """The succeeding proportion of the current window, rounded to two decimals."""
        with self._lock:
            return self._stats.success_rate()

    def remaining_delay(self) -> float:
        """Seconds until the breaker will admit a trial execution, else ``0.0``."""
        with self._lock:
            if self._state != OPEN:
                return 0.0
            return max(0.0, self.delay_seconds - (self._clock.now() - self._opened_at))

    def try_acquire_permit(self) -> bool:
        """Take permission to execute, returning whether it was granted.

        Mutating: an open breaker whose delay has elapsed becomes half-open here, and a
        half-open breaker spends one of its bounded trial permits. Pair every granted
        permit with `record_success` or `record_failure`.
        """
        events: list[tuple[Listeners, StateChangedEvent]] = []
        with self._lock:
            granted = self._try_acquire_locked(events)
        _emit(events, self.on_state_change)
        return granted

    def record_success(self) -> None:
        """Record a successful execution, closing a half-open breaker if enough have."""
        self._record(True, None)

    def record_failure(self) -> None:
        """Record a failed execution, opening the breaker if the threshold is met."""
        self._record(False, None)

    def record(self, outcome: Outcome[Any]) -> None:
        """Record an outcome, judged by this breaker's ``handle`` condition."""
        self._record(not self._handle(outcome.value, outcome.exc), None)

    def open(self) -> None:
        """Force the breaker open, as if the failure threshold had been met."""
        events: list[tuple[Listeners, StateChangedEvent]] = []
        with self._lock:
            self._open_locked(None, events)
        _emit(events, self.on_state_change)

    def half_open(self) -> None:
        """Force the breaker half-open, admitting a bounded number of trials."""
        events: list[tuple[Listeners, StateChangedEvent]] = []
        with self._lock:
            self._half_open_locked(events)
        _emit(events, self.on_state_change)

    def close(self) -> None:
        """Force the breaker closed, discarding the current window."""
        events: list[tuple[Listeners, StateChangedEvent]] = []
        with self._lock:
            self._close_locked(events)
        _emit(events, self.on_state_change)

    def apply(self, inner: Step) -> Step:
        """Wrap ``inner`` so executions are gated by the breaker's state."""

        def step(ex: Execution) -> Generator[Instruction, Any, Outcome[Any]]:
            if not self.try_acquire_permit():
                # A rejected attempt still counts as an attempt, as in Go, but never as
                # an execution: the function was not called.
                ex.attempts += 1
                return Outcome(exc=CircuitBreakerOpen(self.remaining_delay()))

            outcome = yield from inner(ex)
            failed = self._handle(outcome.value, outcome.exc)
            if failed:
                if self.on_failure:
                    self.on_failure.emit(ExecutionEvent(ex, outcome.value, outcome.exc))
            elif self.on_success:
                self.on_success.emit(ExecutionEvent(ex, outcome.value, outcome.exc))
            self._record(not failed, ex)
            return outcome

        return step

    def _record(self, success: bool, ex: Execution | None) -> None:
        events: list[tuple[Listeners, StateChangedEvent]] = []
        with self._lock:
            self._stats.record(success)
            if self._state == HALF_OPEN:
                self._check_half_open_locked(ex, events)
                # Refunded unconditionally, so a trial that decided nothing frees its
                # slot for the next caller.
                self._permits += 1
            elif self._state == CLOSED:
                self._check_closed_locked(ex, events)
        _emit(events, self.on_state_change)

    def _try_acquire_locked(self, events: list[tuple[Listeners, StateChangedEvent]]) -> bool:
        if self._state == OPEN:
            if self._clock.now() - self._opened_at < self.delay_seconds:
                return False
            self._half_open_locked(events)
        if self._state == HALF_OPEN:
            if self._permits <= 0:
                return False
            self._permits -= 1
        return True

    def _check_closed_locked(
        self, ex: Execution | None, events: list[tuple[Listeners, StateChangedEvent]]
    ) -> None:
        if self._stats.executions < self._min_executions:
            return
        if self._failure_rate:
            if self._stats.failure_rate() >= self._failure_rate:
                self._open_locked(ex, events)
        elif self._stats.failures >= self._failure_threshold:
            self._open_locked(ex, events)

    def _check_half_open_locked(
        self, ex: Execution | None, events: list[tuple[Listeners, StateChangedEvent]]
    ) -> None:
        if self._success_threshold:
            successes_met = self._stats.successes >= self._success_threshold
            failures_met = self._stats.failures > self._success_capacity - self._success_threshold
        elif self._failure_rate:
            enough = self._stats.executions >= self._min_executions
            successes_met = enough and self._stats.success_rate() > 1 - self._failure_rate
            failures_met = enough and self._stats.failure_rate() >= self._failure_rate
        else:
            successes_met = self._stats.successes > self._capacity - self._failure_threshold
            failures_met = self._stats.failures >= self._failure_threshold
        if successes_met:
            self._close_locked(events)
        elif failures_met:
            self._open_locked(ex, events)

    def _open_locked(
        self, ex: Execution | None, events: list[tuple[Listeners, StateChangedEvent]]
    ) -> None:
        if self._state == OPEN:
            return
        previous, self._state = self._state, OPEN
        self.delay_seconds = self._delay(ex)
        self._opened_at = self._clock.now()
        events.append((self.on_open, StateChangedEvent(previous, OPEN)))

    def _half_open_locked(self, events: list[tuple[Listeners, StateChangedEvent]]) -> None:
        if self._state == HALF_OPEN:
            return
        previous, self._state = self._state, HALF_OPEN
        # Go's newHalfOpenState: the first non-zero of the success capacity, the rate's
        # execution threshold, then the failure capacity. It sizes both the trial ring
        # and the admission cap, so the breaker can see every trial it admitted.
        capacity = self._success_capacity or self._min_executions or self._capacity
        self._stats = CountingStats(capacity)
        self._permits = capacity
        events.append((self.on_half_open, StateChangedEvent(previous, HALF_OPEN)))

    def _close_locked(self, events: list[tuple[Listeners, StateChangedEvent]]) -> None:
        if self._state == CLOSED:
            return
        previous, self._state = self._state, CLOSED
        self._stats = self._new_stats()
        events.append((self.on_close, StateChangedEvent(previous, CLOSED)))

    def _new_stats(self) -> Stats:
        """Build the closed-state window, which is time-based only if a Rate is set."""
        if self._period is not None:
            return TimedStats(self._period, _BUCKETS, self._clock)
        return CountingStats(self._capacity)


def _emit(events: list[tuple[Listeners, StateChangedEvent]], on_state_change: Listeners) -> None:
    """Fire state-change listeners once the lock is released, so they may re-enter."""
    for listeners, event in events:
        listeners.emit(event)
        on_state_change.emit(event)
