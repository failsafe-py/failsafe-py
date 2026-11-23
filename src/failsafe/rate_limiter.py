"""Smooth and bursty rate limiting."""

from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from collections.abc import Generator
from datetime import timedelta
from typing import Any

from ._core import SYSTEM_CLOCK, Clock, Execution, Instruction, Outcome, Sleep, Step
from ._delays import seconds
from ._events import ExecutionEvent, Listener, Listeners
from ._exceptions import RateLimitExceeded

__all__ = ["RateLimiter"]

_NANOS = 1_000_000_000
"""Permit arithmetic runs in integer nanoseconds, as Go's does.

Float seconds would make ``0.3 / 0.1`` land just under 3 and silently drop a period.
"""


class _Stats(ABC):
    """The reservation arithmetic. Returns a wait; never sleeps."""

    __slots__ = ("_clock", "_start")

    def __init__(self, clock: Clock) -> None:
        self._clock = clock
        self._start = clock.now()

    def _elapsed(self) -> int:
        return int((self._clock.now() - self._start) * _NANOS)

    @abstractmethod
    def acquire(self, permits: int, max_wait: int | None) -> int:
        """Reserve ``permits``, returning the nanoseconds to wait, or ``-1`` if refused.

        Args:
            permits: How many permits to reserve.
            max_wait: The longest acceptable wait in nanoseconds, or ``None`` for any.

        Returns:
            Nanoseconds the caller must wait, or ``-1`` if that would exceed ``max_wait``.
        """

    @abstractmethod
    def reset(self) -> None:
        """Discard all reservations and restart the clock."""


def _exceeds(wait: int, max_wait: int | None) -> bool:
    return max_wait is not None and wait > max_wait


class _SmoothStats(_Stats):
    """Evenly spaced permits: tracks the next interval in which a permit is free."""

    __slots__ = ("_interval", "_next_free")

    def __init__(self, interval: int, clock: Clock) -> None:
        super().__init__(clock)
        self._interval = interval
        self._next_free = 0

    def acquire(self, permits: int, max_wait: int | None) -> int:
        current = self._elapsed()
        requested = self._interval * permits
        if current >= self._next_free:
            current_interval = current - current % self._interval
            new_next_free = current_interval + requested
        else:
            new_next_free = self._next_free + requested

        wait = max(new_next_free - current - self._interval, 0)
        if _exceeds(wait, max_wait):
            return -1
        self._next_free = new_next_free
        return wait

    def reset(self) -> None:
        self._start = self._clock.now()
        self._next_free = 0


class _BurstyStats(_Stats):
    """A fixed window: up to ``period_permits`` per period, with no spacing.

    Available permits may go negative, so a large burst makes later callers wait
    several periods.
    """

    __slots__ = ("_available", "_current_period", "_period", "_period_permits")

    def __init__(self, period_permits: int, period: int, clock: Clock) -> None:
        super().__init__(clock)
        self._period_permits = period_permits
        self._period = period
        self._available = period_permits
        self._current_period = 0

    def acquire(self, permits: int, max_wait: int | None) -> int:
        current = self._elapsed()
        new_period = current // self._period

        if self._current_period < new_period:
            elapsed_permits = (new_period - self._current_period) * self._period_permits
            self._current_period = new_period
            if self._available < 0:
                self._available += elapsed_permits
            else:
                self._available = self._period_permits

        wait = 0
        if permits > self._available:
            next_period_time = (self._current_period + 1) * self._period
            deficit = permits - self._available
            additional_periods = deficit // self._period_permits
            if deficit % self._period_permits == 0:
                additional_periods -= 1
            wait = (next_period_time - current) + additional_periods * self._period
            if _exceeds(wait, max_wait):
                return -1

        self._available -= permits
        return wait

    def reset(self) -> None:
        self._start = self._clock.now()
        self._available = self._period_permits
        self._current_period = 0


class RateLimiter:
    """Limits how often executions are permitted, to keep a resource from overload.

    Two flavours, chosen by constructor:

    - **Smooth** (`smooth`) spreads permits evenly, one every interval, so a
      burst of callers is paced rather than admitted together.
    - **Bursty** (`bursty`) refills a fixed allowance at the start of each
      period, permitting bursts up to that allowance.

    A reservation returns how long the caller must wait and immediately counts against
    the configured rate.

    By default an execution that cannot proceed immediately fails with
    [`RateLimitExceeded`][failsafe.RateLimitExceeded]. Pass ``max_wait_time`` to let it wait
        instead.

    One instance models one resource, so share it across every call that reaches it:

    ```python
    limiter = RateLimiter.smooth(100, period=1.0)
    api = Failsafe(limiter)
    ```

    Attributes:
        on_rate_limit_exceeded: Called when an execution is rejected.
    """

    __slots__ = ("_clock", "_lock", "_max_wait", "_stats", "on_rate_limit_exceeded")

    def __init__(
        self,
        stats: _Stats,
        max_wait_time: float | timedelta | None,
        on_rate_limit_exceeded: Listener | None,
        clock: Clock,
    ) -> None:
        self._stats = stats
        self._clock = clock
        # Go's default max wait is 0: any wait at all rejects the execution.
        self._max_wait = 0 if max_wait_time is None else _nanos(max_wait_time, "max_wait_time")
        self._lock = threading.Lock()
        self.on_rate_limit_exceeded = Listeners("on_rate_limit_exceeded", on_rate_limit_exceeded)

    @classmethod
    def smooth(
        cls,
        max_executions: int | None = None,
        period: float | timedelta = 1.0,
        *,
        max_rate: float | timedelta | None = None,
        max_wait_time: float | timedelta | None = None,
        on_rate_limit_exceeded: Listener | None = None,
        clock: Clock = SYSTEM_CLOCK,
    ) -> RateLimiter:
        """Build a limiter that spreads executions evenly over time.

        Give either ``max_executions`` with a ``period``, or ``max_rate`` directly:

        ```python
        RateLimiter.smooth(100, period=1.0)   # 100 per second, one every 10ms
        RateLimiter.smooth(max_rate=0.01)     # the same, stated as a rate
        ```

        Args:
            max_executions: Executions permitted per ``period``.
            period: The period over which ``max_executions`` are spread. Accepts
                seconds or a `timedelta`.
            max_rate: The interval between permitted executions, as an alternative to
                ``max_executions`` and ``period``.
            max_wait_time: How long an execution may wait for a permit before being
                rejected. Applies only to executions run through a
                [`Failsafe`][failsafe.Failsafe], never to the standalone methods.
            on_rate_limit_exceeded: Called when an execution is rejected.
            clock: Custom time source for permit scheduling (e.g. for testing).

        Returns:
            A smooth rate limiter.

        Raises:
            ValueError: If neither or both of ``max_executions`` and ``max_rate`` are
                given, or if a value is not positive.
        """
        if (max_executions is None) == (max_rate is None):
            raise ValueError("give exactly one of max_executions or max_rate")
        if max_rate is not None:
            interval = _nanos(max_rate, "max_rate")
        else:
            assert max_executions is not None
            if max_executions <= 0:
                raise ValueError("max_executions must be positive")
            interval = _nanos(period, "period") // max_executions
        if interval <= 0:
            raise ValueError("the interval between executions must be positive")
        return cls(_SmoothStats(interval, clock), max_wait_time, on_rate_limit_exceeded, clock)

    @classmethod
    def bursty(
        cls,
        max_executions: int,
        period: float | timedelta = 1.0,
        *,
        max_wait_time: float | timedelta | None = None,
        on_rate_limit_exceeded: Listener | None = None,
        clock: Clock = SYSTEM_CLOCK,
    ) -> RateLimiter:
        """Build a limiter that permits bursts of executions within each period.

        ``RateLimiter.bursty(10, period=1.0)`` permits ten executions per second, all
        ten of which may run at once. A new period resets the allowance.

        Args:
            max_executions: Executions permitted in each period.
            period: The length of a period. Accepts seconds or a
                `timedelta`.
            max_wait_time: How long an execution may wait for a permit before being
                rejected. Applies only to executions run through a
                [`Failsafe`][failsafe.Failsafe], never to the standalone methods.
            on_rate_limit_exceeded: Called when an execution is rejected.
            clock: Custom time source for permit scheduling (e.g. for testing).

        Returns:
            A bursty rate limiter.

        Raises:
            ValueError: If ``max_executions`` or ``period`` is not positive.
        """
        if max_executions <= 0:
            raise ValueError("max_executions must be positive")
        stats = _BurstyStats(max_executions, _nanos(period, "period"), clock)
        return cls(stats, max_wait_time, on_rate_limit_exceeded, clock)

    def reserve_permit(self, permits: int = 1) -> float:
        """Reserve permits, returning the seconds to wait before using them.

        The permits are reserved whether or not the caller waits, so a caller that
        ignores the returned wait exceeds the configured rate.

        Args:
            permits: How many permits to reserve.

        Returns:
            Seconds to wait, or ``0.0`` if the permits are free now.
        """
        return self._reserve(permits, None)

    def try_reserve_permit(
        self, max_wait_time: float | timedelta, permits: int = 1
    ) -> float | None:
        """Reserve permits if the wait is acceptable.

        Args:
            max_wait_time: The longest acceptable wait.
            permits: How many permits to reserve.

        Returns:
            Seconds to wait, ``0.0`` if the permits are free now, or ``None`` if the
            wait would be too long and no permits were reserved.
        """
        wait = self._reserve(permits, _nanos(max_wait_time, "max_wait_time"))
        return None if wait < 0 else wait

    def try_acquire_permit(self, permits: int = 1) -> bool:
        """Acquire permits only if they are free right now.

        Args:
            permits: How many permits to acquire.

        Returns:
            Whether the permits were acquired.
        """
        return self._reserve(permits, 0) == 0.0

    def acquire_permit(self, permits: int = 1) -> None:
        """Acquire permits, blocking the calling thread until they are available.

        Never rejects: ``max_wait_time`` applies only to executions run through a
        [`Failsafe`][failsafe.Failsafe]. Use `try_reserve_permit` for a standalone
        bound on the wait.

        Args:
            permits: How many permits to acquire.
        """
        wait = self._reserve(permits, None)
        if wait > 0:
            self._clock.sleep(wait)

    async def acquire_permit_async(self, permits: int = 1) -> None:
        """Acquire permits, suspending the calling task until they are available.

        Args:
            permits: How many permits to acquire.
        """
        wait = self._reserve(permits, None)
        if wait > 0:
            await self._clock.sleep_async(wait)

    def reset(self) -> None:
        """Discard all reservations, as if the limiter had just been built."""
        with self._lock:
            self._stats.reset()

    def _reserve(self, permits: int, max_wait: int | None) -> float:
        if permits <= 0:
            raise ValueError("permits must be positive")
        with self._lock:
            wait = self._stats.acquire(permits, max_wait)
        return -1.0 if wait < 0 else wait / _NANOS

    def apply(self, inner: Step) -> Step:
        """Wrap ``inner`` so each execution acquires a permit first."""

        def step(ex: Execution) -> Generator[Instruction, Any, Outcome[Any]]:
            with self._lock:
                wait = self._stats.acquire(1, self._max_wait)
            if wait < 0:
                self.on_rate_limit_exceeded.emit(ExecutionEvent(ex, None, None))
                return Outcome(exc=RateLimitExceeded(self._max_wait / _NANOS))
            if wait > 0:
                yield Sleep(wait / _NANOS)
            return (yield from inner(ex))

        return step


def _nanos(value: float | timedelta, name: str) -> int:
    """Convert seconds or a timedelta to non-negative integer nanoseconds."""
    total = round(seconds(value) * _NANOS)
    if total < 0:
        raise ValueError(f"{name} must not be negative")
    return total
