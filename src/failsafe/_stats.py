"""Execution result windows shared by the stateful policies.

Ports Go's ``internal/util``: a fixed-size ring (``countingStats``) and a rolling
bucketed window over a time period (``timedStats``). Neither is concurrency safe; the
owning policy holds its lock.
"""

from __future__ import annotations

import collections
import math

from ._core import Clock

__all__ = ["CountingStats", "Stats", "TimedStats", "TimedUsage", "round2"]


def round2(value: float) -> float:
    """Round half away from zero to two decimals, as Go's ``util.Round``.

    `round` would use banker's rounding, which disagrees with Go at exactly .005.
    """
    return math.floor(value * 100 + 0.5) / 100


class CountingStats:
    """A ring of the last ``capacity`` results, as Go's ``countingStats``."""

    __slots__ = ("_capacity", "_results", "failures", "successes")

    def __init__(self, capacity: int) -> None:
        self._capacity = capacity
        self._results: collections.deque[bool] = collections.deque(maxlen=capacity)
        self.successes = 0
        self.failures = 0

    def record(self, success: bool) -> None:
        """Add a result, evicting the oldest once the ring is full."""
        if len(self._results) == self._capacity:
            evicted = self._results[0]
            if evicted:
                self.successes -= 1
            else:
                self.failures -= 1
        self._results.append(success)
        if success:
            self.successes += 1
        else:
            self.failures += 1

    @property
    def executions(self) -> int:
        """How many results the ring currently holds."""
        return len(self._results)

    def failure_rate(self) -> float:
        """The share of failures, rounded as Go rounds."""
        return round2(self.failures / self.executions) if self.executions else 0.0

    def success_rate(self) -> float:
        """The share of successes, rounded as Go rounds."""
        return round2(self.successes / self.executions) if self.executions else 0.0


class TimedStats:
    """Results within a rolling period, bucketed as Go's ``timedStats``.

    Results age out one bucket at a time rather than resetting all at once. Buckets are
    expired on write, as Go does, so a stale window still reads its last totals until
    the next record.
    """

    __slots__ = ("_bucket", "_buckets", "_clock", "_count", "_head", "failures", "successes")

    def __init__(self, period: float, buckets: int, clock: Clock) -> None:
        self._clock = clock
        self._count = buckets
        self._bucket = period / buckets
        self._buckets: list[list[int]] = [[0, 0] for _ in range(buckets)]
        self._head = int(clock.now() / self._bucket)
        self.successes = 0
        self.failures = 0

    def _current(self) -> list[int]:
        """Expire buckets that have rolled out of the window, as Go does on write."""
        head = int(self._clock.now() / self._bucket)
        if head > self._head:
            for i in range(min(self._count, head - self._head)):
                bucket = self._buckets[(self._head + i + 1) % self._count]
                self.successes -= bucket[0]
                self.failures -= bucket[1]
                bucket[0] = bucket[1] = 0
            self._head = head
        return self._buckets[self._head % self._count]

    def record(self, success: bool) -> None:
        """Add a result to the current bucket."""
        bucket = self._current()
        if success:
            bucket[0] += 1
            self.successes += 1
        else:
            bucket[1] += 1
            self.failures += 1

    @property
    def executions(self) -> int:
        """How many results fall within the window."""
        return self.successes + self.failures

    def failure_rate(self) -> float:
        """The share of failures, rounded as Go rounds."""
        return round2(self.failures / self.executions) if self.executions else 0.0

    def success_rate(self) -> float:
        """The share of successes, rounded as Go rounds."""
        return round2(self.successes / self.executions) if self.executions else 0.0


class TimedUsage:
    """A total over a rolling period, bucketed as `TimedStats` is.

    Backs the usage tracker's per-user totals, so a user who was heavy an hour ago is
    not still paying for it.
    """

    __slots__ = ("_bucket", "_buckets", "_clock", "_count", "_head", "total")

    def __init__(self, period: float, buckets: int, clock: Clock) -> None:
        self._clock = clock
        self._count = buckets
        self._bucket = period / buckets
        self._buckets = [0] * buckets
        self._head = int(clock.now() / self._bucket)
        self.total = 0

    def expire(self) -> None:
        """Drop buckets that have rolled out of the window."""
        head = int(self._clock.now() / self._bucket)
        if head > self._head:
            for i in range(min(self._count, head - self._head)):
                index = (self._head + i + 1) % self._count
                self.total -= self._buckets[index]
                self._buckets[index] = 0
            self._head = head

    def record(self, usage: int) -> None:
        """Add ``usage`` to the current bucket."""
        self.expire()
        self._buckets[self._head % self._count] += usage
        self.total += usage


Stats = CountingStats | TimedStats
"""Either window shape, as the circuit breaker swaps between them per state."""
