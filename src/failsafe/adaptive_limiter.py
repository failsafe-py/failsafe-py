"""Adaptive concurrency limiting, ported from failsafe-go's ``adaptivelimiter``."""

from __future__ import annotations

import math
import random
import threading
from collections.abc import Generator, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from ._core import SYSTEM_CLOCK, Clock, Execution, Instruction, Outcome, Step
from ._delays import seconds
from ._events import ExecutionEvent, Listener, Listeners
from ._exceptions import LimitExceeded
from ._prioritizer import Prioritizer, Stats
from ._semaphore import FifoSemaphore
from ._windows import (
    CorrelationWindow,
    MaxWindow,
    MovingAverage,
    MovingMedian,
    QuantileWindow,
    log10_step,
)
from .priority import Priority

__all__ = ["AdaptiveLimiter", "LimitChangedEvent", "queue_prioritizer"]

_WARMUP_SAMPLES = 10
"""Go's warmupSamples: averages use a simple mean until this many samples land."""

_SMOOTHED_SAMPLES = 5
"""Go's smoothedSamples: the median filter and smoothing window size."""

_ALPHA_FACTOR = 3
"""Queue sizes below ``alpha`` mean there is room to grow."""

_BETA_FACTOR = 6
"""Queue sizes above ``beta`` mean the limit is already too high."""

_STEP_FACTOR = 1
"""How far the limit moves per adjustment, before log scaling."""


@dataclass(frozen=True, slots=True)
class LimitChangedEvent:
    """An :class:`AdaptiveLimiter`'s concurrency limit changed.

    Attributes:
        old_limit: The limit before the change.
        new_limit: The limit after the change.
    """

    old_limit: int
    new_limit: int


@dataclass(frozen=True, slots=True)
class _QueueStats:
    """A limiter's queueing, as reported to a :class:`~failsafe._prioritizer.Prioritizer`."""

    limit: int
    queued: int
    rejection_threshold: int
    max_queue: int

    def compute_rejection_rate(self) -> float:
        """The share of executions to reject, scaling with how full the queue is."""
        if self.queued < self.rejection_threshold:
            return 0.0
        if self.queued >= self.max_queue:
            return 1.0
        span = self.max_queue - self.rejection_threshold
        return (self.queued - self.rejection_threshold) / span


class _QueueRejectionStrategy:
    """Sums queueing across limiters, so rejection reflects total system pressure."""

    def combine(self, stats: Sequence[Stats]) -> _QueueStats:
        """Add up every limiter's queue depth and capacity."""
        limit = queued = threshold = max_queue = 0
        for entry in stats:
            assert isinstance(entry, _QueueStats)
            limit += entry.limit
            queued += entry.queued
            threshold += entry.rejection_threshold
            max_queue += entry.max_queue
        return _QueueStats(limit, queued, threshold, max_queue)


def queue_prioritizer(
    *,
    level_tracker: Any = None,
    usage_tracker: Any = None,
    on_threshold_changed: Listener | None = None,
) -> Prioritizer:
    """Create a :class:`~failsafe._prioritizer.Prioritizer` driven by limiter queueing.

    Args:
        level_tracker: Where accepted levels are recorded. Shared if given.
        usage_tracker: Ranks users by usage so heavy users shed first. Optional.
        on_threshold_changed: Called when the rejection threshold moves.

    Returns:
        A prioritizer for :class:`AdaptiveLimiter` instances.
    """
    return Prioritizer(
        _QueueRejectionStrategy(),
        level_tracker=level_tracker,
        usage_tracker=usage_tracker,
        on_threshold_changed=on_threshold_changed,
    )


class _Permit:
    """A held permit, which records its execution's duration when released."""

    __slots__ = ("_inflight", "_limiter", "_start", "_user_id")

    def __init__(
        self, limiter: AdaptiveLimiter, start: float, inflight: int, user_id: str | None
    ) -> None:
        self._limiter = limiter
        self._start = start
        self._inflight = inflight
        self._user_id = user_id

    def record(self) -> None:
        """Record a completed execution, releasing the permit."""
        now = self._limiter._clock.now()
        self._limiter._record(now, now - self._start, self._inflight, dropped=False)
        if self._user_id is not None:
            tracker = self._limiter._usage_tracker
            if tracker is not None:
                tracker.record_usage(self._user_id, int((now - self._start) * 1e9))

    def drop(self) -> None:
        """Release the permit without recording a sample.

        Used when an execution's duration says nothing about the backend — a rejection
        before the call ran, for instance — so it must not move the limit.
        """
        self._limiter._record(self._limiter._clock.now(), 0.0, self._inflight, dropped=True)


class AdaptiveLimiter:
    """Limits concurrency to what a resource can actually sustain, and keeps adjusting.

    A :class:`~failsafe.Bulkhead` enforces a limit you picked; this one discovers it.
    Recent execution times are compared against a baseline average: when they trend up
    the limit falls, when they trend down it rises. During overload it converges on the
    machine's real capacity, which no fixed number tracks across deploys and traffic
    shifts.

    Executions beyond the limit raise :exc:`~failsafe.LimitExceeded` unless queueing is
    enabled, which lets a bounded number wait instead of failing outright.

    One instance models one resource, so share it across every call that reaches it::

        limiter = AdaptiveLimiter()
        failsafe = Failsafe(limiter)

    Args:
        min_limit: The floor for the concurrency limit.
        max_limit: The ceiling for the concurrency limit.
        initial_limit: Where the limit starts.
        max_limit_factor: The limit may not exceed this multiple of recent inflight
            executions, which stops it drifting up while traffic is light.
        max_limit_factor_decay: Shrinks ``max_limit_factor`` logarithmically as inflight
            grows, so headroom tightens under heavy load. ``0`` disables decay.
        min_limit_factor: The floor for a decayed ``max_limit_factor``.
        max_limit_stabilization_window: Smooths the inflight count that
            ``max_limit_factor`` applies to, over this many seconds, so a brief dip in
            traffic cannot pull the limit down.
        recent_window_min_duration: The shortest interval between limit updates.
        recent_window_max_duration: The longest interval between limit updates.
        recent_window_min_samples: Executions required before the limit may update.
        recent_quantile: Which quantile of recent execution times to compare against
            the baseline. Defaults to ``0.9``, so tail latency drives the decision.
        baseline_window_age: How far back the baseline average remembers.
        correlation_window_size: How many samples feed the throughput and latency
            correlations that detect overload.
        queueing: Enables queueing as an ``(initial_rejection_factor,
            max_rejection_factor)`` pair, both multiples of the current limit:
            executions start being rejected once the queue passes the first, and all
            are rejected at the second.
        max_wait_time: How long a queued execution may wait. ``None`` waits
            indefinitely.
        on_limit_exceeded: Called when an execution is rejected.
        on_limit_changed: Called when the concurrency limit moves.
        clock: The time source. Injectable so tests need not sleep.
        rng: The source of rejection randomness. Injectable so tests are deterministic.

    Raises:
        ValueError: If the limits are inconsistent or a factor is out of range.
    """

    __slots__ = (
        "_baseline_rtt",
        "_clock",
        "_correlation_window_size",
        "_initial_limit",
        "_last_max_inflight",
        "_limit",
        "_lock",
        "_max_inflight_window",
        "_max_limit",
        "_max_limit_factor",
        "_max_limit_factor_decay",
        "_max_rejection_factor",
        "_max_wait_time",
        "_median_filter",
        "_min_limit",
        "_min_limit_factor",
        "_next_update_time",
        "_prioritizer",
        "_recent_quantile",
        "_recent_rtt",
        "_recent_window_max_duration",
        "_recent_window_min_duration",
        "_recent_window_min_samples",
        "_rejection_factor",
        "_rng",
        "_rtt_correlation",
        "_semaphore",
        "_smoothed_recent_rtt",
        "_throughput_correlation",
        "_usage_tracker",
        "on_limit_changed",
        "on_limit_exceeded",
    )

    def __init__(
        self,
        *,
        min_limit: int = 1,
        max_limit: int = 200,
        initial_limit: int = 20,
        max_limit_factor: float = 5.0,
        max_limit_factor_decay: float = 0.0,
        min_limit_factor: float = 1.0,
        max_limit_stabilization_window: float | timedelta = 0.0,
        recent_window_min_duration: float | timedelta = 1.0,
        recent_window_max_duration: float | timedelta = 30.0,
        recent_window_min_samples: int = 50,
        recent_quantile: float = 0.9,
        baseline_window_age: int = 10,
        correlation_window_size: int = 50,
        queueing: tuple[float, float] | None = None,
        max_wait_time: float | timedelta | None = None,
        on_limit_exceeded: Listener | None = None,
        on_limit_changed: Listener | None = None,
        clock: Clock = SYSTEM_CLOCK,
        rng: random.Random | None = None,
    ) -> None:
        if not 0 < min_limit <= initial_limit <= max_limit:
            raise ValueError("limits must satisfy 0 < min_limit <= initial_limit <= max_limit")
        if max_limit_factor < 1:
            raise ValueError("max_limit_factor must be at least 1")
        if not 0 <= recent_quantile <= 1:
            raise ValueError("recent_quantile must be between 0 and 1")
        min_duration = seconds(recent_window_min_duration)
        max_duration = seconds(recent_window_max_duration)
        if min_duration > max_duration:
            raise ValueError("recent_window_min_duration must not exceed its max")
        stabilization = seconds(max_limit_stabilization_window)
        if stabilization < 0:
            raise ValueError("max_limit_stabilization_window must not be negative")
        if queueing is not None and not 1 <= queueing[0] <= queueing[1]:
            raise ValueError("queueing factors must each be at least 1, with initial <= max")

        self._min_limit = min_limit
        self._max_limit = max_limit
        self._initial_limit = initial_limit
        self._max_limit_factor = max_limit_factor
        self._max_limit_factor_decay = max_limit_factor_decay
        self._min_limit_factor = min_limit_factor
        self._recent_window_min_duration = min_duration
        self._recent_window_max_duration = max_duration
        self._recent_window_min_samples = recent_window_min_samples
        self._recent_quantile = recent_quantile
        self._correlation_window_size = correlation_window_size
        self._rejection_factor, self._max_rejection_factor = queueing if queueing else (0.0, 0.0)
        self._max_wait_time = seconds(max_wait_time) if max_wait_time is not None else None
        self._clock = clock
        self._rng = rng if rng is not None else random.Random()

        self._lock = threading.Lock()
        self._semaphore = FifoSemaphore(initial_limit)
        self._limit = float(initial_limit)
        self._last_max_inflight = 0
        self._recent_rtt = QuantileWindow()
        self._median_filter = MovingMedian(_SMOOTHED_SAMPLES)
        self._smoothed_recent_rtt = MovingAverage(_SMOOTHED_SAMPLES, _WARMUP_SAMPLES)
        self._baseline_rtt = MovingAverage(baseline_window_age, _WARMUP_SAMPLES)
        self._max_inflight_window = MaxWindow(stabilization)
        self._rtt_correlation = CorrelationWindow(correlation_window_size, _WARMUP_SAMPLES)
        self._throughput_correlation = CorrelationWindow(correlation_window_size, _WARMUP_SAMPLES)
        self._next_update_time = clock.now()
        self._prioritizer: Prioritizer | None = None
        self._usage_tracker: Any = None

        self.on_limit_exceeded = Listeners("on_limit_exceeded", on_limit_exceeded)
        self.on_limit_changed = Listeners("on_limit_changed", on_limit_changed)

    def prioritized_by(self, prioritizer: Prioritizer) -> AdaptiveLimiter:
        """Reject by priority level during overload, coordinated by ``prioritizer``.

        Queueing is required for prioritized rejection and is enabled with Go's
        defaults if it was not configured.

        Args:
            prioritizer: The prioritizer to register with. Share one across policies to
                shed load based on total system pressure.

        Returns:
            This limiter, for chaining.
        """
        if self._rejection_factor == 0 and self._max_rejection_factor == 0:
            self._rejection_factor, self._max_rejection_factor = 2.0, 3.0
        self._prioritizer = prioritizer
        self._usage_tracker = prioritizer.usage_tracker
        prioritizer.register(self._queue_stats)
        return self

    @property
    def limit(self) -> int:
        """The current concurrency limit."""
        with self._lock:
            return int(self._limit)

    @property
    def inflight(self) -> int:
        """How many executions are running right now."""
        return self._semaphore.used

    @property
    def queued(self) -> int:
        """How many executions are waiting for a permit."""
        return self._semaphore.waiters

    @property
    def max_inflight(self) -> int:
        """The peak inflight count during the last sampling window."""
        with self._lock:
            return self._last_max_inflight

    @property
    def queueing(self) -> bool:
        """Whether executions may queue when the limiter is full."""
        return self._max_rejection_factor > 0

    def _queue_stats(self) -> _QueueStats:
        limit = self.limit
        return _QueueStats(
            limit=limit,
            queued=self.queued,
            rejection_threshold=int(limit * self._rejection_factor),
            max_queue=int(limit * self._max_rejection_factor),
        )

    def can_acquire_permit(self, priority: Priority | None = None) -> bool:
        """Whether a permit could be acquired right now.

        Args:
            priority: A priority to judge against when prioritized, overriding any in
                scope.

        Returns:
            Whether an execution would be admitted.
        """
        if self._prioritizer is not None:
            return self._can_acquire_at_level(self._prioritizer.level_for(priority))
        if not self._semaphore.is_full:
            return True
        if not self.queueing:
            return False
        rate = self._queue_stats().compute_rejection_rate()
        if rate == 0:
            return True
        return not (rate >= 1 or rate >= self._rng.random())

    def _can_acquire_at_level(self, level: int) -> bool:
        assert self._prioritizer is not None
        if self._prioritizer.registered_policies == 1:
            if not self._semaphore.is_full:
                return True
            if self.queued >= int(self.limit * self._max_rejection_factor):
                return False
        return level >= self._prioritizer.rejection_threshold

    def _admit(self, priority: Priority | None) -> int | None:
        """Decide whether to admit an execution, returning its level if prioritized."""
        if self._prioritizer is None:
            if not self.can_acquire_permit():
                raise LimitExceeded(self.limit)
            return None
        level = self._prioritizer.level_for(priority)
        if not self._can_acquire_at_level(level):
            raise LimitExceeded(self.limit)
        if self._semaphore.is_full:
            self._prioritizer.level_tracker.record_level(level)
        return level

    def try_acquire_permit(self) -> _Permit | None:
        """Acquire a permit only if one is free right now.

        Returns:
            The permit, or ``None`` if the limiter was full.
        """
        if not self._semaphore.acquire(timeout=0):
            return None
        return self._new_permit()

    def acquire_permit(self, priority: Priority | None = None) -> _Permit:
        """Acquire a permit, blocking the calling thread if queueing is enabled.

        Args:
            priority: A priority to judge against, overriding any in scope.

        Returns:
            A permit whose :meth:`_Permit.record` must be called when the execution ends.

        Raises:
            LimitExceeded: If the execution was rejected, or waited too long.
        """
        self._admit(priority)
        if not self._semaphore.acquire(self._wait_timeout()):
            raise LimitExceeded(self.limit)
        return self._new_permit()

    async def acquire_permit_async(self, priority: Priority | None = None) -> _Permit:
        """Acquire a permit, suspending the calling task if queueing is enabled.

        Args:
            priority: A priority to judge against, overriding any in scope.

        Returns:
            A permit whose :meth:`_Permit.record` must be called when the execution ends.

        Raises:
            LimitExceeded: If the execution was rejected, or waited too long.
        """
        self._admit(priority)
        if not await self._semaphore.acquire_async(self._wait_timeout()):
            raise LimitExceeded(self.limit)
        return self._new_permit()

    def _wait_timeout(self) -> float | None:
        if not self.queueing:
            return 0.0
        return self._max_wait_time

    def _new_permit(self) -> _Permit:
        user_id = None
        if self._usage_tracker is not None:
            from .priority import current_user

            user_id = current_user()
        return _Permit(self, self._clock.now(), self._semaphore.used, user_id)

    def reset(self) -> None:
        """Return the limit and every window to their initial state."""
        with self._lock:
            self._semaphore.resize(self._initial_limit)
            self._limit = float(self._initial_limit)
            self._last_max_inflight = 0
            self._recent_rtt.reset()
            self._median_filter.reset()
            self._smoothed_recent_rtt.reset()
            self._baseline_rtt.reset()
            self._max_inflight_window.reset()
            self._rtt_correlation.reset()
            self._throughput_correlation.reset()
            self._next_update_time = self._clock.now()

    def _record(self, now: float, rtt: float, inflight: int, *, dropped: bool) -> None:
        """Record an execution's duration, updating the limit when the window is full."""
        event = None
        with self._lock:
            if not dropped:
                self._recent_rtt.add(rtt, inflight)

            if (
                now > self._next_update_time
                and self._recent_rtt.size >= self._recent_window_min_samples
            ):
                quantile = self._recent_rtt.quantile(self._recent_quantile)
                filtered = self._median_filter.add(quantile)
                smoothed = self._smoothed_recent_rtt.add(filtered)
                self._last_max_inflight = self._recent_rtt.max_inflight
                event = self._update_limit(smoothed, self._recent_rtt.max_inflight, now)
                min_rtt = self._recent_rtt.min_rtt
                self._recent_rtt.reset()
                window = max(min_rtt * 2, self._recent_window_min_duration)
                self._next_update_time = now + min(window, self._recent_window_max_duration)

        self._semaphore.release()
        if event is not None:
            self.on_limit_changed.emit(event)

    def _update_limit(
        self, recent_rtt: float, inflight: int, now: float
    ) -> LimitChangedEvent | None:
        """Move the limit based on the gradient between recent and baseline times."""
        baseline_rtt = self._baseline_rtt.add(recent_rtt)
        gradient = baseline_rtt / recent_rtt if recent_rtt else 1.0
        queue_size = math.ceil(self._limit * (1 - gradient))

        throughput = inflight / recent_rtt if recent_rtt else 0.0
        throughput_corr, _, throughput_cv = self._throughput_correlation.add(inflight, throughput)
        rtt_corr, _, _ = self._rtt_correlation.add(inflight, recent_rtt)

        overloaded = self._semaphore.is_full
        limit = int(self._limit)
        alpha = log10_step(_ALPHA_FACTOR, limit)
        beta = log10_step(_BETA_FACTOR, limit)

        old_limit = self._limit
        new_limit = old_limit
        if _should_decrease(queue_size, beta, overloaded, throughput_corr, throughput_cv, rtt_corr):
            new_limit = old_limit - log10_step(_STEP_FACTOR, int(old_limit))
        elif queue_size < alpha:
            new_limit = old_limit + log10_step(_STEP_FACTOR, int(old_limit))

        max_inflight = inflight
        if self._max_inflight_window.configured:
            max_inflight = self._max_inflight_window.add(inflight, now)

        max_limit = self._compute_max_limit(max_inflight)
        if new_limit > max_limit:
            if old_limit > max_limit:
                new_limit = old_limit - log10_step(_STEP_FACTOR, int(old_limit))
            else:
                new_limit = max_limit

        new_limit = min(max(new_limit, self._min_limit), self._max_limit)
        self._semaphore.resize(int(new_limit))
        self._limit = new_limit

        if int(old_limit) != int(new_limit):
            return LimitChangedEvent(int(old_limit), int(new_limit))
        return None

    def _compute_max_limit(self, inflight: int) -> float:
        """The ceiling implied by recent inflight, with optional logarithmic decay."""
        factor = self._max_limit_factor
        if self._max_limit_factor_decay > 0 and inflight > 0:
            factor = self._max_limit_factor - self._max_limit_factor_decay * math.log10(inflight)
            factor = max(factor, self._min_limit_factor)
        return inflight * factor

    def apply(self, inner: Step) -> Step:
        """Wrap ``inner`` so each attempt holds a permit for its duration."""

        def step(ex: Execution) -> Generator[Instruction, Any, Outcome[Any]]:
            try:
                permit = self.acquire_permit()
            except LimitExceeded as exc:
                self.on_limit_exceeded.emit(ExecutionEvent(ex, None, None))
                return Outcome(exc=exc)
            try:
                return (yield from inner(ex))
            finally:
                permit.record()

        return step


def _should_decrease(
    queue_size: int,
    beta: int,
    overloaded: bool,
    throughput_corr: float,
    throughput_cv: float,
    rtt_corr: float,
) -> bool:
    """Whether the signals say the limit is too high, as Go's ``computeChange``.

    Beyond the queue-size threshold, three correlation checks catch moderate overload
    that queue size alone misses: latency climbing with concurrency while throughput
    stays flat means the limit is already past the resource's capacity.
    """
    if queue_size > beta:
        return True
    if not overloaded:
        return False
    if throughput_corr < 0:
        return True
    if throughput_corr < 0.3 and rtt_corr > 0.5:
        return True
    return throughput_cv < 0.2 and rtt_corr > 0.5
