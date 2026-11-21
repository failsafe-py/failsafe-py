"""Concurrency limiting, ported from failsafe-go's ``bulkhead``."""

from __future__ import annotations

from collections.abc import Generator, Sequence
from datetime import timedelta
from typing import Any

from ._core import Acquire, Execution, Instruction, Outcome, Step
from ._delays import seconds
from ._events import ExecutionEvent, Listener, Listeners
from ._exceptions import BulkheadFull
from ._semaphore import FifoSemaphore

__all__ = ["Bulkhead"]


class Bulkhead:
    """Limits how many executions may run at once, to bound load on a resource.

    An execution that finds every permit taken waits up to ``max_wait_time`` and then
    fails with :exc:`~failsafe.BulkheadFull`. The default wait of zero rejects it
    immediately.

    The permit is held for a single *attempt* and released when that attempt finishes,
    so a :class:`~failsafe.RetryPolicy` composed outside a bulkhead re-acquires for
    each retry rather than holding a permit across the delay between them.

    A single instance serves both sync and async callers with one permit count, and
    hands permits to waiters in arrival order (D-14)::

        bulkhead = Bulkhead(10)
        api = Failsafe(bulkhead)

    Args:
        max_concurrency: How many executions may run at once.
        max_wait_time: How long an execution waits for a permit before being rejected.
            Accepts seconds or a :class:`~datetime.timedelta`. Defaults to no wait.
        on_full: Called when an execution is rejected because no permit was free.

    Raises:
        ValueError: If ``max_concurrency`` is not positive, or ``max_wait_time`` is
            negative.

    Attributes:
        on_full: Called when an execution is rejected.
    """

    __slots__ = ("_max_concurrency", "_max_wait", "_semaphore", "on_full")

    def __init__(
        self,
        max_concurrency: int,
        *,
        max_wait_time: float | timedelta = 0.0,
        on_full: Listener | Sequence[Listener] | None = None,
    ) -> None:
        if max_concurrency <= 0:
            raise ValueError("max_concurrency must be positive")
        wait = seconds(max_wait_time)
        if wait < 0:
            raise ValueError("max_wait_time must not be negative")
        self._max_concurrency = max_concurrency
        self._max_wait = wait
        self._semaphore = FifoSemaphore(max_concurrency)
        self.on_full = Listeners("on_full", on_full)

    @property
    def max_concurrency(self) -> int:
        """How many executions may run at once."""
        return self._max_concurrency

    @property
    def used(self) -> int:
        """How many permits are currently held."""
        return self._semaphore.used

    @property
    def waiters(self) -> int:
        """How many callers are blocked waiting for a permit."""
        return self._semaphore.waiters

    def try_acquire_permit(self) -> bool:
        """Acquire a permit only if one is free right now.

        Returns:
            Whether a permit was acquired. Release it with :meth:`release_permit`.
        """
        return self._semaphore.acquire(timeout=0)

    def acquire_permit(self, timeout: float | timedelta | None = None) -> None:
        """Acquire a permit, blocking the calling thread until one is available.

        Args:
            timeout: Seconds to wait, or ``None`` to wait indefinitely. Note that the
                policy's ``max_wait_time`` applies to executions run through a
                :class:`~failsafe.Failsafe`, not to this method.

        Raises:
            BulkheadFull: If no permit became available within ``timeout``.
        """
        if not self._semaphore.acquire(None if timeout is None else seconds(timeout)):
            raise BulkheadFull(self._max_concurrency)

    async def acquire_permit_async(self, timeout: float | timedelta | None = None) -> None:
        """Acquire a permit, suspending the calling task until one is available.

        Args:
            timeout: Seconds to wait, or ``None`` to wait indefinitely.

        Raises:
            BulkheadFull: If no permit became available within ``timeout``.
        """
        acquired = await self._semaphore.acquire_async(
            None if timeout is None else seconds(timeout)
        )
        if not acquired:
            raise BulkheadFull(self._max_concurrency)

    def release_permit(self) -> None:
        """Release a permit back to the bulkhead.

        Raises:
            RuntimeError: If called without a matching successful acquire.
        """
        self._semaphore.release()

    def apply(self, inner: Step) -> Step:
        """Wrap ``inner`` so each attempt holds a permit for its duration."""

        def step(ex: Execution) -> Generator[Instruction, Any, Outcome[Any]]:
            acquired = yield Acquire(self._semaphore, self._max_wait)
            if not acquired:
                self.on_full.emit(ExecutionEvent(ex, None, None))
                return Outcome(exc=BulkheadFull(self._max_concurrency))
            try:
                return (yield from inner(ex))
            finally:
                self._semaphore.release()

        return step
