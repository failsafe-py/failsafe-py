"""The rejection threshold shared by prioritized policies.

Ports failsafe-go's ``internal.BasePrioritizer``. A prioritizer gathers queueing or
rejection statistics from every policy registered with it and, on each
:meth:`Prioritizer.calibrate`, turns them into one rejection rate and the level below
which executions are rejected. Sharing one prioritizer across several policies lets
them shed load based on overall system pressure rather than each one's local view.
"""

from __future__ import annotations

import contextlib
import logging
import threading
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from typing import Protocol

from ._events import Listener, Listeners
from .priority import (
    LevelTracker,
    Priority,
    UsageTracker,
    current_level,
    current_priority,
    current_user,
)

__all__ = ["Prioritizer", "RejectionStrategy", "Stats", "ThresholdChangedEvent"]

_logger = logging.getLogger("failsafe.prioritizer")


@dataclass(frozen=True, slots=True)
class ThresholdChangedEvent:
    """A :class:`Prioritizer`'s rejection threshold changed.

    Attributes:
        old_threshold: The level below which executions were previously rejected.
        new_threshold: The level below which executions are now rejected.
    """

    old_threshold: int
    new_threshold: int


class Stats(Protocol):
    """Statistics a policy reports to its prioritizer."""

    def compute_rejection_rate(self) -> float:
        """The share of executions that should be rejected, from ``0`` to ``1``."""
        ...


class RejectionStrategy(Protocol):
    """Combines per-policy statistics into one system-wide view."""

    def combine(self, stats: Sequence[Stats]) -> Stats:
        """Merge each registered policy's statistics.

        Args:
            stats: One entry per registered policy.

        Returns:
            The combined statistics.
        """
        ...


class Prioritizer:
    """Decides which priority levels to reject while the system is overloaded.

    Policies register themselves; :meth:`calibrate` then samples them all and sets the
    rejection threshold from the recorded distribution of levels. Nothing recalculates
    on its own — call :meth:`calibrate` on an interval, or use
    :meth:`scheduled_calibrations` to run it on a background thread.

    Args:
        strategy: How to combine registered policies' statistics.
        level_tracker: Where accepted levels are recorded. Share one across policies to
            pool their level distributions.
        usage_tracker: Ranks users by usage so heavy users are shed first. Optional.
        on_threshold_changed: Called when the rejection threshold moves.
    """

    __slots__ = (
        "_level_tracker",
        "_lock",
        "_rejection_rate",
        "_stats_funcs",
        "_strategy",
        "_threshold",
        "_usage_tracker",
        "on_threshold_changed",
    )

    def __init__(
        self,
        strategy: RejectionStrategy,
        *,
        level_tracker: LevelTracker | None = None,
        usage_tracker: UsageTracker | None = None,
        on_threshold_changed: Listener | None = None,
    ) -> None:
        self._strategy = strategy
        self._level_tracker = level_tracker if level_tracker is not None else LevelTracker()
        self._usage_tracker = usage_tracker
        self._lock = threading.Lock()
        self._stats_funcs: list[Callable[[], Stats]] = []
        self._rejection_rate = 0.0
        self._threshold = 0
        self.on_threshold_changed = Listeners("on_threshold_changed", on_threshold_changed)

    @property
    def level_tracker(self) -> LevelTracker:
        """Where accepted execution levels are recorded."""
        return self._level_tracker

    @property
    def usage_tracker(self) -> UsageTracker | None:
        """The per-user usage ranking, if one was configured."""
        return self._usage_tracker

    @property
    def rejection_rate(self) -> float:
        """The share of executions being rejected, from ``0`` to ``1``."""
        with self._lock:
            return self._rejection_rate

    @property
    def rejection_threshold(self) -> int:
        """The level below which executions are rejected."""
        with self._lock:
            return self._threshold

    @property
    def registered_policies(self) -> int:
        """How many policies report to this prioritizer."""
        with self._lock:
            return len(self._stats_funcs)

    def register(self, stats_func: Callable[[], Stats]) -> None:
        """Register a policy's statistics source.

        Args:
            stats_func: Called during calibration to sample the policy.
        """
        with self._lock:
            self._stats_funcs.append(stats_func)

    def calibrate(self) -> None:
        """Recompute the rejection rate and threshold from registered policies."""
        with self._lock:
            combined = self._strategy.combine([func() for func in self._stats_funcs])
            self._rejection_rate = combined.compute_rejection_rate()
            new_threshold = (
                self._level_tracker.get_level(self._rejection_rate)
                if self._rejection_rate > 0
                else 0
            )
            old_threshold = self._threshold
            self._threshold = new_threshold
            rate = self._rejection_rate

        _logger.debug("prioritizer calibration: rate=%.2f threshold=%d", rate, new_threshold)
        if old_threshold != new_threshold:
            self.on_threshold_changed.emit(ThresholdChangedEvent(old_threshold, new_threshold))
        if self._usage_tracker is not None:
            self._usage_tracker.calibrate()

    @contextlib.contextmanager
    def scheduled_calibrations(self, interval: float) -> Iterator[None]:
        """Calibrate every ``interval`` seconds for the duration of the block.

        Args:
            interval: Seconds between calibrations.

        Raises:
            ValueError: If the interval is not positive.
        """
        if interval <= 0:
            raise ValueError("interval must be positive")
        stop = threading.Event()

        def run() -> None:
            while not stop.wait(interval):
                self.calibrate()

        thread = threading.Thread(target=run, name="failsafe-prioritizer", daemon=True)
        thread.start()
        try:
            yield
        finally:
            stop.set()
            thread.join()

    def level_for(self, priority: Priority | None = None) -> int:
        """The level for the execution in scope.

        A user in scope with a :class:`~failsafe.priority.UsageTracker` configured takes
        precedence, so heavy users rank below light ones within the same priority. Then
        an explicit level or priority from the surrounding scope, and finally ``0``.

        Args:
            priority: A priority to fall back on when none is in scope.

        Returns:
            A level from ``0`` to ``499``.
        """
        if self._usage_tracker is not None:
            effective = priority if priority is not None else current_priority()
            if effective is not None:
                user_id = current_user()
                if user_id:
                    return self._usage_tracker.get_level(user_id, effective)
        level = current_level()
        if level is not None:
            return level
        if priority is not None:
            return priority.random_level()
        return 0


def _current_priority() -> Priority | None:
    from .priority import current_priority

    return current_priority()
