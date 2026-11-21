"""Fails an execution that takes longer than a fixed time limit."""

from __future__ import annotations

from collections.abc import Generator, Sequence
from datetime import timedelta
from typing import Any

from ._core import Execution, Instruction, Outcome, Race, Step
from ._delays import seconds
from ._events import ExecutionCompletedEvent, Listener, Listeners
from ._exceptions import TimeoutExceeded

__all__ = ["Timeout"]


class Timeout:
    """Fails an execution that exceeds a time limit.

    Composition order decides what the limit covers. Outside a retry it bounds all
    attempts together; inside one it bounds each attempt separately::

        Failsafe(Timeout(10.0), retry)   # 10s for everything
        Failsafe(retry, Timeout(10.0))   # 10s per attempt

    :exc:`~failsafe.TimeoutExceeded` is an ordinary failure, so an outer
    :class:`~failsafe.RetryPolicy` can retry it. An :exc:`asyncio.CancelledError`
    cannot: that is terminal.

    **Sync mode is cooperative by default.** Python cannot interrupt a running call, so
    the limit is enforced between instructions, and a function that blocks for a minute
    under a one-second timeout still blocks for a minute. ``interrupt=True`` runs the
    attempt on a worker thread and frees the caller on time, at the cost of leaving
    that thread running to completion. Async mode always interrupts, via
    :func:`asyncio.timeout`.

    Args:
        timeout: The limit, in seconds or as a timedelta.
        interrupt: Run sync attempts on a worker thread so the caller is freed on time.
            Ignored in async mode, which always interrupts.
        on_timeout: Called when the limit is exceeded.

    Raises:
        ValueError: If ``timeout`` is not positive.
    """

    __slots__ = ("interrupt", "on_timeout", "timeout")

    def __init__(
        self,
        timeout: float | timedelta,
        *,
        interrupt: bool = False,
        on_timeout: Listener | Sequence[Listener] | None = None,
    ) -> None:
        limit = seconds(timeout)
        if limit <= 0:
            raise ValueError("timeout must be positive")
        self.timeout = limit
        """The configured limit in seconds."""
        self.interrupt = interrupt
        """Whether sync attempts run on a worker thread."""
        self.on_timeout = Listeners("on_timeout", on_timeout)
        """Listeners called when the limit is exceeded."""

    def apply(self, inner: Step) -> Step:
        """Wrap ``inner`` in a race against the time limit."""

        def step(ex: Execution) -> Generator[Instruction, Any, Outcome[Any]]:
            outcome = yield Race(inner, self.timeout, self.interrupt)
            if outcome is not None:
                return outcome  # type: ignore[no-any-return]
            exceeded = TimeoutExceeded(self.timeout)
            if self.on_timeout:
                self.on_timeout.emit(ExecutionCompletedEvent(ex, None, exceeded))
            # Deliberately replaces whatever the inner policies produced: an inner
            # fallback may still have been processing a result when time ran out, and
            # it must be interrupted rather than allowed to win.
            return Outcome(exc=exceeded)

        return step
