"""Client-side adaptive throttling, as described in the Google SRE book."""

from __future__ import annotations

import random
import threading
from collections.abc import Generator, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from ._conditions import UNSET, Condition, FailureConditions
from ._core import SYSTEM_CLOCK, Clock, Execution, Instruction, Outcome, Step
from ._delays import seconds
from ._events import ExecutionEvent, Listener, Listeners
from ._exceptions import ThrottleExceeded
from ._prioritizer import Prioritizer, Stats
from ._stats import TimedStats
from .priority import LevelTracker, Priority, UsageTracker

__all__ = ["AdaptiveThrottler", "throttle_prioritizer"]

_BUCKETS = 20
"""Go's adaptivethrottler bucket count, twice the circuit breaker's."""

_EXECUTION_PADDING = 1
"""Go's executionPadding: keeps the rejection rate below 1 at low execution counts."""


def _compute_rejection_rate(
    executions: float,
    successes: float,
    success_rate_threshold: float,
    max_rejection_rate: float,
    execution_threshold: int,
) -> float:
    """Rejection probability from recent results, as Go's ``computeRejectionRate``.

    Rejections ramp up once the observed success rate falls below the threshold: the
    client stops sending the excess load the backend has shown it cannot serve.

    Args:
        executions: Executions recorded within the window.
        successes: Successes recorded within the window.
        success_rate_threshold: The success rate the backend is expected to sustain.
        max_rejection_rate: The ceiling on the returned rate.
        execution_threshold: Executions required before anything is rejected.

    Returns:
        A rate from ``0`` to ``max_rejection_rate``.
    """
    if executions < execution_threshold:
        return 0.0
    max_allowed = successes / success_rate_threshold
    excess = max(0.0, executions - max_allowed)
    return min(excess / (executions + _EXECUTION_PADDING), max_rejection_rate)


@dataclass(frozen=True, slots=True)
class _ThrottlerStats:
    """A throttler's recent rejection pressure, as reported to a prioritizer."""

    executions: float
    rejection_rate: float
    max_rejection_rate: float

    def compute_rejection_rate(self) -> float:
        """The rejection rate, never above the throttler's ceiling."""
        return min(self.rejection_rate, self.max_rejection_rate)


class _ThrottlerRejectionStrategy:
    """Averages rejection rates across throttlers, weighted by execution count."""

    def combine(self, stats: Sequence[Stats]) -> _ThrottlerStats:
        """Weight each throttler's rate by how much traffic it actually saw."""
        if not stats:
            return _ThrottlerStats(0.0, 0.0, 0.0)
        total_executions = 0.0
        weighted = 0.0
        min_max_rate = 1.0
        for entry in stats:
            assert isinstance(entry, _ThrottlerStats)
            total_executions += entry.executions
            weighted += entry.rejection_rate * entry.executions
            min_max_rate = min(min_max_rate, entry.max_rejection_rate)
        rate = weighted / total_executions if total_executions > 0 else 0.0
        return _ThrottlerStats(total_executions, rate, min_max_rate)


def throttle_prioritizer(
    *,
    level_tracker: LevelTracker | None = None,
    usage_tracker: UsageTracker | None = None,
    on_threshold_changed: Listener | None = None,
) -> Prioritizer:
    """Create a :class:`~failsafe._prioritizer.Prioritizer` driven by throttler failures.

    Args:
        level_tracker: Where accepted levels are recorded. Shared if given.
        usage_tracker: Ranks users by usage so heavy users shed first. Optional.
        on_threshold_changed: Called when the rejection threshold moves.

    Returns:
        A prioritizer for :class:`AdaptiveThrottler` instances.
    """
    return Prioritizer(
        _ThrottlerRejectionStrategy(),
        level_tracker=level_tracker,
        usage_tracker=usage_tracker,
        on_threshold_changed=on_threshold_changed,
    )


class AdaptiveThrottler:
    """Rejects a growing share of executions while a resource keeps failing.

    Unlike a :class:`~failsafe.CircuitBreaker`, which is open or closed for everyone,
    the throttler rejects *probabilistically*: as the recent failure rate climbs past
    ``failure_rate_threshold``, each execution is likelier to be rejected with
    :exc:`~failsafe.ThrottleExceeded`. Load sheds in proportion to the damage, and a
    recovering backend keeps receiving probe traffic because ``max_rejection_rate``
    stops short of rejecting everything.

    The window is time-based: results age out gradually over ``thresholding_period``
    rather than resetting, so a backend that recovers is not held back by old failures.

    One instance models one resource, so share it across every call that reaches that
    resource::

        throttler = AdaptiveThrottler(failure_rate_threshold=0.2)
        payments = Failsafe(retry, throttler)

    Args:
        failure_rate_threshold: The failure rate, from ``0`` to ``1``, above which
            executions start being rejected. Defaults to ``0.1``.
        execution_threshold: Executions required within the period before anything is
            rejected, so a single early failure cannot throttle a quiet resource.
        thresholding_period: The rolling window over which results count. Accepts
            seconds or a :class:`~datetime.timedelta`. Defaults to one minute.
        max_rejection_rate: The ceiling on the rejection rate, from ``0`` to ``1``.
            Defaults to ``0.9``, leaving a tenth of traffic to probe for recovery.
        handle: What counts as a failure. Defaults to any exception.
        on_success: Called when an execution run through the throttler succeeds.
        on_failure: Called when an execution run through the throttler fails.
        clock: The time source. Injectable so tests need not sleep.
        rng: The source of rejection randomness. Injectable so tests are deterministic.

    Raises:
        ValueError: If a rate is outside ``0`` to ``1``, or the period is not positive.
    """

    __slots__ = (
        "_clock",
        "_execution_threshold",
        "_is_failure",
        "_lock",
        "_max_rejection_rate",
        "_prioritizer",
        "_rejection_rate",
        "_rng",
        "_stats",
        "_success_rate_threshold",
        "on_failure",
        "on_success",
    )

    def __init__(
        self,
        *,
        failure_rate_threshold: float = 0.1,
        execution_threshold: int = 0,
        thresholding_period: float | timedelta = 60.0,
        max_rejection_rate: float = 0.9,
        handle: Condition = UNSET,
        on_success: Listener | None = None,
        on_failure: Listener | None = None,
        clock: Clock = SYSTEM_CLOCK,
        rng: random.Random | None = None,
    ) -> None:
        if not 0 <= failure_rate_threshold <= 1:
            raise ValueError("failure_rate_threshold must be between 0 and 1")
        if not 0 <= max_rejection_rate <= 1:
            raise ValueError("max_rejection_rate must be between 0 and 1")
        period = seconds(thresholding_period)
        if period <= 0:
            raise ValueError("thresholding_period must be positive")
        if execution_threshold < 0:
            raise ValueError("execution_threshold must not be negative")

        self._success_rate_threshold = min(1.0, max(0.0, 1 - failure_rate_threshold))
        self._execution_threshold = execution_threshold
        self._max_rejection_rate = max_rejection_rate
        self._is_failure = FailureConditions(handle)
        self._clock = clock
        self._rng = rng if rng is not None else random.Random()
        self._lock = threading.Lock()
        self._stats = TimedStats(period, _BUCKETS, clock)
        self._rejection_rate = 0.0
        self.on_success = Listeners("on_success", on_success)
        self.on_failure = Listeners("on_failure", on_failure)
        self._prioritizer: Prioritizer | None = None

    def prioritized_by(self, prioritizer: Prioritizer) -> AdaptiveThrottler:
        """Reject by priority level instead of uniformly at random.

        Args:
            prioritizer: The prioritizer to register with. Share one across policies to
                shed load based on total system pressure.

        Returns:
            This throttler, for chaining.
        """
        self._prioritizer = prioritizer
        prioritizer.register(self._throttler_stats)
        return self

    def _throttler_stats(self) -> _ThrottlerStats:
        with self._lock:
            return _ThrottlerStats(
                executions=float(self._stats.executions),
                rejection_rate=_compute_rejection_rate(
                    self._stats.executions,
                    self._stats.successes,
                    self._success_rate_threshold,
                    self._max_rejection_rate,
                    self._execution_threshold,
                ),
                max_rejection_rate=self._max_rejection_rate,
            )

    @property
    def rejection_rate(self) -> float:
        """The share of executions currently being rejected, from ``0`` to ``1``.

        Recomputed by each permit acquisition, so it reflects the last decision rather
        than the state of the window right now.
        """
        with self._lock:
            return self._rejection_rate

    @property
    def executions(self) -> int:
        """Executions recorded within the thresholding period."""
        with self._lock:
            return self._stats.executions

    @property
    def successes(self) -> int:
        """Successes recorded within the thresholding period."""
        with self._lock:
            return self._stats.successes

    @property
    def failures(self) -> int:
        """Failures recorded within the thresholding period."""
        with self._lock:
            return self._stats.failures

    def _should_reject_locked(self, priority: Priority | None = None) -> bool:
        """Recompute the rejection rate and draw against it. Assumes the lock is held."""
        self._rejection_rate = _compute_rejection_rate(
            self._stats.executions,
            self._stats.successes,
            self._success_rate_threshold,
            self._max_rejection_rate,
            self._execution_threshold,
        )
        if self._prioritizer is not None:
            return self._should_reject_by_level(priority)
        if self._rejection_rate == 0:
            return False
        return self._rejection_rate >= 1 or self._rejection_rate >= self._rng.random()

    def _should_reject_by_level(self, priority: Priority | None) -> bool:
        """Reject below the prioritizer's threshold, letting a trickle through anyway."""
        assert self._prioritizer is not None
        level = self._prioritizer.level_for(priority)
        if level >= self._prioritizer.rejection_threshold:
            self._prioritizer.level_tracker.record_level(level)
            return False
        # Even fully throttled, some share is admitted so low priorities cannot starve.
        if self._rng.random() < 1.0 - self._max_rejection_rate:
            self._prioritizer.level_tracker.record_level(level)
            return False
        return True

    def try_acquire_permit(self, priority: Priority | None = None) -> bool:
        """Attempt to acquire a permit, without raising.

        Args:
            priority: A priority to judge against when prioritized, overriding any in
                scope.

        Returns:
            ``True`` if the execution may proceed.
        """
        with self._lock:
            return not self._should_reject_locked(priority)

    def acquire_permit(self, priority: Priority | None = None) -> None:
        """Acquire a permit to execute, or raise.

        Never blocks: the decision is immediate. The async form is identical, and
        exists so callers need not know which one they hold.

        Args:
            priority: A priority to judge against when prioritized, overriding any in
                scope.

        Raises:
            ThrottleExceeded: If the execution was rejected.
        """
        with self._lock:
            rejected = self._should_reject_locked(priority)
            rate = self._rejection_rate
        if rejected:
            raise ThrottleExceeded(rate)

    async def acquire_permit_async(self, priority: Priority | None = None) -> None:
        """Acquire a permit to execute, or raise.

        Args:
            priority: A priority to judge against when prioritized, overriding any in
                scope.

        Raises:
            ThrottleExceeded: If the execution was rejected.
        """
        self.acquire_permit(priority)

    def record_success(self) -> None:
        """Record an execution success."""
        with self._lock:
            self._stats.record(True)

    def record_failure(self) -> None:
        """Record an execution failure."""
        with self._lock:
            self._stats.record(False)

    def record_result(self, outcome: Outcome[Any]) -> None:
        """Record an outcome, judged by this throttler's ``handle`` condition.

        Args:
            outcome: The result to record.
        """
        with self._lock:
            self._stats.record(not self._is_failure(outcome.value, outcome.exc))

    def apply(self, inner: Step) -> Step:
        """Wrap ``inner`` so executions are throttled and their results recorded."""

        def step(ex: Execution) -> Generator[Instruction, Any, Outcome[Any]]:
            with self._lock:
                rejected = self._should_reject_locked()
                rate = self._rejection_rate
            if rejected:
                return Outcome(exc=ThrottleExceeded(rate))

            outcome = yield from inner(ex)
            failure = self._is_failure(outcome.value, outcome.exc)
            with self._lock:
                self._stats.record(not failure)
            listeners = self.on_failure if failure else self.on_success
            listeners.emit(ExecutionEvent(ex, outcome.value, outcome.exc))
            return outcome

        return step
