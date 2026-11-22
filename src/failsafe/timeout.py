"""Fails an execution that takes longer than a fixed time limit."""

from __future__ import annotations

from collections.abc import Generator
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
    attempts together; inside one it bounds each attempt separately:

    ```python
    Failsafe(Timeout(10.0), retry)   # 10s for everything
    Failsafe(retry, Timeout(10.0))   # 10s per attempt
    ```

    [`TimeoutExceeded`][failsafe.TimeoutExceeded] is an ordinary failure, so an outer
    [`RetryPolicy`][failsafe.RetryPolicy] can retry it. An `asyncio.CancelledError`
    cannot: that is terminal.

    **Async interrupts the attempt; sync cannot.** Under `AsyncFailsafe` the awaiting
    attempt is interrupted by `asyncio.timeout`. Under
    [`Failsafe`][failsafe.Failsafe] the limit is checked between instructions, so a
    blocking call runs to completion and the timeout is only observed once it returns.
    A sync callee makes the limit real by passing
    [`Execution.remaining()`][failsafe.Execution.remaining] to whatever blocks.

    Args:
        timeout: The limit, in seconds or as a timedelta.
        on_timeout: Called when the limit is exceeded.

    Raises:
        ValueError: If ``timeout`` is not positive.
    """

    __slots__ = ("on_timeout", "timeout")

    def __init__(
        self,
        timeout: float | timedelta,
        *,
        on_timeout: Listener | None = None,
    ) -> None:
        limit = seconds(timeout)
        if limit <= 0:
            raise ValueError("timeout must be positive")
        self.timeout = limit
        """The configured limit in seconds."""
        self.on_timeout = Listeners("on_timeout", on_timeout)
        """Listeners called when the limit is exceeded."""

    def apply(self, inner: Step) -> Step:
        """Wrap ``inner`` in a race against the time limit."""

        def step(ex: Execution) -> Generator[Instruction, Any, Outcome[Any]]:
            outcome = yield Race(inner, self.timeout)
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
