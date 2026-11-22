"""Execution prioritization, ported from failsafe-go's ``priority`` package.

During overload a prioritized policy sheds load from the bottom up: each execution
carries a *level*, and the [`Prioritizer`][failsafe.Prioritizer] computes the level below which
executions are rejected. Levels run from ``0`` to ``499``, with each
[`Priority`][failsafe.Priority] owning a hundred of them, so a subset of a priority can be shed
rather than the whole class at once.

Priority is ambient rather than a parameter: it is request-scoped data
that propagates across hops, so it lives in a `ContextVar` and is
read by whichever policy happens to run:

```python
with priority_scope(Priority.HIGH):
    failsafe.run(charge_card)
```
"""

from __future__ import annotations

import bisect
import collections
import contextlib
import contextvars
import enum
import math
import random
import threading
from collections.abc import Iterator

from ._core import SYSTEM_CLOCK, Clock
from ._stats import TimedUsage, round2

__all__ = [
    "LevelTracker",
    "Priority",
    "UsageTracker",
    "current_level",
    "current_user",
    "level_scope",
    "priority_scope",
    "user_scope",
]

TOTAL_LEVELS = 500
"""The number of distinct priority levels, spread evenly across the priorities."""

_LEVELS_PER_PRIORITY = 100
_MAX_LEVEL_OFFSET = 99.0


class Priority(enum.IntEnum):
    """An execution priority, from `VERY_LOW` to `VERY_HIGH`.

    Each priority spans a hundred levels. Rejections are made by level, so load within
    a single priority sheds gradually instead of all at once.
    """

    VERY_LOW = 0
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    VERY_HIGH = 4

    @property
    def min_level(self) -> int:
        """The lowest level belonging to this priority."""
        return self.value * _LEVELS_PER_PRIORITY

    @property
    def max_level(self) -> int:
        """The highest level belonging to this priority."""
        return self.min_level + _LEVELS_PER_PRIORITY - 1

    def random_level(self, rng: random.Random | None = None) -> int:
        """Pick a level uniformly from this priority's range.

        Spreading executions across the range is what lets a prioritizer shed part of a
        priority: without it every execution in a class would be accepted or rejected
        together.

        Args:
            rng: Custom source of randomness (e.g. for testing).

        Returns:
            A level within this priority.
        """
        source = rng if rng is not None else random
        return source.randint(self.min_level, self.max_level)


_PRIORITY: contextvars.ContextVar[Priority] = contextvars.ContextVar("failsafe_priority")
_LEVEL: contextvars.ContextVar[int] = contextvars.ContextVar("failsafe_level")
_USER: contextvars.ContextVar[str] = contextvars.ContextVar("failsafe_user")


@contextlib.contextmanager
def priority_scope(priority: Priority) -> Iterator[None]:
    """Run the enclosed block at ``priority``.

    Args:
        priority: The priority to apply to executions within the block.
    """
    token = _PRIORITY.set(priority)
    try:
        yield
    finally:
        _PRIORITY.reset(token)


@contextlib.contextmanager
def level_scope(level: int) -> Iterator[None]:
    """Run the enclosed block at an exact ``level``, bypassing random level selection.

    Args:
        level: The level to apply, from ``0`` to ``499``.

    Raises:
        ValueError: If the level is outside the valid range.
    """
    if not 0 <= level < TOTAL_LEVELS:
        raise ValueError(f"level must be between 0 and {TOTAL_LEVELS - 1}")
    token = _LEVEL.set(level)
    try:
        yield
    finally:
        _LEVEL.reset(token)


@contextlib.contextmanager
def user_scope(user_id: str) -> Iterator[None]:
    """Attribute the enclosed block's usage to ``user_id``.

    Only meaningful with a [`UsageTracker`][failsafe.UsageTracker], which uses the
    attribution to give light users a better level than heavy ones.

    Args:
        user_id: The user to attribute usage to.
    """
    token = _USER.set(user_id)
    try:
        yield
    finally:
        _USER.reset(token)


def current_priority() -> Priority | None:
    """Return the [`Priority`][failsafe.Priority] in scope, or ``None``."""
    return _PRIORITY.get(None)


def current_level(rng: random.Random | None = None) -> int | None:
    """Return the level in scope.

    An exact level set by [`level_scope`][failsafe.level_scope] wins; otherwise a random
    level within the
    [`Priority`][failsafe.Priority] in scope is returned, or ``None`` if neither is set.

    Args:
        rng: The source of randomness for priority-derived levels.

    Returns:
        A level from ``0`` to ``499``, or ``None``.
    """
    level = _LEVEL.get(None)
    if level is not None:
        return level
    priority = _PRIORITY.get(None)
    if priority is not None:
        return priority.random_level(rng)
    return None


def current_user() -> str | None:
    """Return the user ID in scope, or ``None``."""
    return _USER.get(None)


class LevelTracker:
    """Tracks the levels of recently accepted executions.

    A [`Prioritizer`][failsafe.Prioritizer] asks this for the level at a given quantile:
    to reject 30% of load it needs the level that 30% of recent executions fall below.

    Args:
        window_size: How many recent levels to retain.
    """

    __slots__ = ("_counts", "_lock", "_window")

    def __init__(self, window_size: int = 1000) -> None:
        if window_size <= 0:
            raise ValueError("window_size must be positive")
        self._lock = threading.Lock()
        self._window: collections.deque[int] = collections.deque(maxlen=window_size)
        self._counts = [0] * TOTAL_LEVELS

    def record_level(self, level: int) -> None:
        """Record that an execution at ``level`` was accepted."""
        with self._lock:
            if len(self._window) == self._window.maxlen:
                self._counts[self._window[0]] -= 1
            self._window.append(level)
            self._counts[level] += 1

    def get_level(self, quantile: float) -> int:
        """The level at ``quantile`` among recorded levels, or ``0`` if none.

        Args:
            quantile: The share of executions that should fall below the result.

        Returns:
            A level from ``0`` to ``499``.
        """
        with self._lock:
            size = len(self._window)
            if size == 0:
                return 0
            target = max(1, math.ceil(size * quantile))
            counted = 0
            for level, count in enumerate(self._counts):
                counted += count
                if counted >= target:
                    return level
            return 0

    def reset(self) -> None:
        """Forget every recorded level."""
        with self._lock:
            self._window.clear()
            self._counts = [0] * TOTAL_LEVELS


_USAGE_BUCKETS = 30
"""Go's usage window bucket count."""


class _UserEntry:
    """A single user's recent usage and its standing among all users."""

    __slots__ = ("last_active", "quantile", "window")

    def __init__(self, window: TimedUsage, now: float) -> None:
        self.window = window
        self.last_active = now
        self.quantile = -1.0


class UsageTracker:
    """Ranks users by recent resource usage, so heavy users are shed first.

    Without this, every execution at a priority competes on an equal footing and one
    heavy user can crowd out many light ones. With it, a user's level within their
    priority falls as their usage rises relative to everyone else's.

    Usage is attributed via [`user_scope`][failsafe.user_scope] and totals are only re-ranked by
    `calibrate`.

    Args:
        window: The span in seconds over which usage counts. Users idle for twice this
            long are forgotten. Defaults to five minutes.
        max_users: The most users to track at once; the least recently active is
            evicted beyond that.
        clock: Custom time source for usage windows (e.g. for testing).

    Raises:
        ValueError: If the window or user limit is not positive.
    """

    __slots__ = ("_clock", "_lock", "_max_users", "_users", "_window")

    def __init__(
        self,
        window: float = 300.0,
        max_users: int = 10_000,
        clock: Clock = SYSTEM_CLOCK,
    ) -> None:
        if window <= 0:
            raise ValueError("window must be positive")
        if max_users <= 0:
            raise ValueError("max_users must be positive")
        self._window = window
        self._max_users = max_users
        self._clock = clock
        self._lock = threading.Lock()
        self._users: collections.OrderedDict[str, _UserEntry] = collections.OrderedDict()

    def record_usage(self, user_id: str, usage: int) -> None:
        """Add ``usage`` to a user's running total.

        Args:
            user_id: The user to attribute usage to.
            usage: The amount to add, in whatever unit the caller tracks. The limiter
                records execution duration in nanoseconds.
        """
        with self._lock:
            entry = self._users.get(user_id)
            if entry is None:
                if len(self._users) >= self._max_users:
                    self._users.popitem(last=False)
                entry = _UserEntry(
                    TimedUsage(self._window, _USAGE_BUCKETS, self._clock), self._clock.now()
                )
                self._users[user_id] = entry
            else:
                self._users.move_to_end(user_id)
            entry.last_active = self._clock.now()
            entry.window.record(usage)

    def get_usage(self, user_id: str) -> int | None:
        """The user's usage within the window, or ``None`` if they are not tracked."""
        with self._lock:
            entry = self._users.get(user_id)
            return None if entry is None else entry.window.total

    def get_level(self, user_id: str, priority: Priority) -> int:
        """The level for a user within ``priority``, based on their relative usage.

        An unknown, idle, or not-yet-calibrated user gets the priority's top level:
        usage has to be observed and ranked before it can count against anyone.

        Args:
            user_id: The user to rank.
            priority: The priority whose level range to place them in.

        Returns:
            A level within the priority's range.
        """
        with self._lock:
            entry = self._users.get(user_id)
            if entry is None or entry.window.total == 0 or entry.quantile < 0:
                return priority.max_level
            fairness = 1.0 - entry.quantile
            return priority.min_level + int(_MAX_LEVEL_OFFSET * fairness)

    def calibrate(self) -> None:
        """Re-rank every tracked user and forget the long-idle ones.

        Ranking only happens here, so a prioritizer that never calibrates leaves every
        user at their priority's top level.
        """
        with self._lock:
            cutoff = self._clock.now() - 2 * self._window
            for entry in self._users.values():
                entry.window.expire()
            for user_id in [
                user_id
                for user_id, entry in self._users.items()
                if entry.window.total == 0 and entry.last_active < cutoff
            ]:
                del self._users[user_id]
            usages = sorted(e.window.total for e in self._users.values() if e.window.total > 0)
            for entry in self._users.values():
                if entry.window.total > 0:
                    rank = bisect.bisect_left(usages, entry.window.total) / len(usages)
                    entry.quantile = round2(rank)
                else:
                    entry.quantile = -1.0
