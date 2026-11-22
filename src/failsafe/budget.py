"""A shared ceiling on retries and hedges, ported from failsafe-go's ``budget``."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ._events import Listener, Listeners

if TYPE_CHECKING:
    from ._core import Execution

__all__ = ["Budget", "BudgetExceededEvent"]


@dataclass(frozen=True, slots=True)
class BudgetExceededEvent:
    """A retry or hedge was refused because the budget was spent.

    Attributes:
        execution: The execution that was refused.
        kind: Either ``"retry"`` or ``"hedge"``.
        rate: The rate of extra attempts at the moment of refusal.
    """

    execution: Execution
    kind: str
    rate: float


class Budget:
    """Caps the share of in-flight work that retries and hedges may consume.

    A retry storm is self-inflicted load: the attempts that were meant to paper over
    a few failures become most of the traffic to an already struggling service. One
    budget shared between policies bounds that, letting extra attempts through only
    while they stay a small fraction of normal traffic:

    ```python
    budget = Budget()
    api = Failsafe(RetryPolicy(budget=budget))
    search = Failsafe(RetryPolicy(budget=budget))
    ```

    Share one instance across the policies guarding the same dependency; a budget
    per policy bounds nothing, since the dependency sees their sum.

    The rate is measured against executions in flight right now rather than a time
    window, so it needs no clock and reacts immediately. Below ``min_concurrency``
    extra attempts are always allowed, so a quiet service still retries.

    When the budget is spent, a [`RetryPolicy`][failsafe.RetryPolicy] stops with
    [`BudgetExceeded`][failsafe.BudgetExceeded] and a [`HedgePolicy`][failsafe.HedgePolicy] simply
    stops hedging, leaving the attempts already running to produce a result.

    Args:
        max_rate: The share of in-flight executions that may be retries or hedges.
        min_concurrency: How many extra attempts are allowed regardless of the rate.
        on_exceeded: Called with a [`BudgetExceededEvent`][failsafe.BudgetExceededEvent] when an
            attempt is
            refused.

    Raises:
        ValueError: If ``max_rate`` is not between 0 and 1, or ``min_concurrency`` is
            negative.

    Attributes:
        on_exceeded: Listeners called when an attempt is refused.
    """

    __slots__ = ("_executions", "_extra", "_lock", "max_rate", "min_concurrency", "on_exceeded")

    def __init__(
        self,
        *,
        max_rate: float = 0.2,
        min_concurrency: int = 3,
        on_exceeded: Listener | None = None,
    ) -> None:
        if not 0 < max_rate <= 1:
            raise ValueError("max_rate must be greater than 0 and at most 1")
        if min_concurrency < 0:
            raise ValueError("min_concurrency must not be negative")
        self.max_rate = max_rate
        """The share of in-flight executions that may be retries or hedges."""
        self.min_concurrency = min_concurrency
        """How many extra attempts are allowed regardless of the rate."""
        self.on_exceeded = Listeners("on_exceeded", on_exceeded)
        """Listeners called when an attempt is refused."""
        self._lock = threading.Lock()
        self._executions = 0
        self._extra = 0

    @property
    def rate(self) -> float:
        """The share of in-flight executions that are currently retries or hedges."""
        with self._lock:
            return self._rate()

    def _rate(self) -> float:
        if self._executions == 0:
            return 0.0
        return self._extra / self._executions

    def record_execution(self) -> None:
        """Count an execution as in flight. Pair with `release_execution`."""
        with self._lock:
            self._executions += 1

    def release_execution(self) -> None:
        """Drop an execution from the in-flight count."""
        with self._lock:
            self._executions -= 1

    def try_acquire_permit(self) -> bool:
        """Admit one retry or hedge, returning whether the budget allowed it.

        A granted permit counts as an extra attempt *and* as an execution, so a
        retry-heavy period raises its own denominator rather than only its numerator.
        Release it with `release_permit`.
        """
        with self._lock:
            if self._extra >= self.min_concurrency and self._rate() > self.max_rate:
                return False
            self._extra += 1
            self._executions += 1
            return True

    def release_permit(self) -> None:
        """Release a permit granted by `try_acquire_permit`."""
        with self._lock:
            self._extra -= 1
            self._executions -= 1

    def notify_exceeded(self, ex: Execution, kind: str) -> None:
        """Emit `on_exceeded` for an attempt the budget refused."""
        if self.on_exceeded:
            self.on_exceeded.emit(BudgetExceededEvent(ex, kind, self.rate))

    def __repr__(self) -> str:
        return (
            f"Budget(max_rate={self.max_rate}, min_concurrency={self.min_concurrency}, "
            f"rate={self.rate:.2f})"
        )
