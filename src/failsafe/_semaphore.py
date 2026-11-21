"""A counting semaphore that blocks threads and asyncio tasks on shared state.

Mirrors failsafe-go's ``internal/util.DynamicSemaphore``, with one addition Go does
not need: a single instance serves both sync and async callers, so a policy built on
it (``Bulkhead``, ``AdaptiveLimiter``) has one permit count rather than two.
"""

from __future__ import annotations

import asyncio
import collections
import threading

__all__ = ["FifoSemaphore"]


class _ThreadWaiter:
    """A blocked thread awaiting a permit."""

    __slots__ = ("_event", "granted")

    def __init__(self) -> None:
        self.granted = False
        self._event = threading.Event()

    def claim(self) -> None:
        """Hand this waiter a permit. Called under the semaphore lock."""
        self.granted = True
        self._event.set()

    def wait(self, timeout: float | None) -> bool:
        """Block until granted a permit, returning False if the timeout elapsed."""
        return self._event.wait(timeout)


class _AsyncWaiter:
    """A blocked asyncio task awaiting a permit."""

    __slots__ = ("_future", "_loop", "granted")

    def __init__(self) -> None:
        self.granted = False
        self._loop = asyncio.get_running_loop()
        self._future: asyncio.Future[bool] = self._loop.create_future()

    def claim(self) -> None:
        """Hand this waiter a permit. Called under the semaphore lock, from any thread."""
        self.granted = True
        self._loop.call_soon_threadsafe(self._resolve)

    def _resolve(self) -> None:
        if not self._future.done():
            self._future.set_result(True)

    async def wait(self, timeout: float | None) -> bool:
        """Suspend until granted a permit, returning False if the timeout elapsed."""
        try:
            # Shielded so a timeout leaves the future intact for the claim/abandon race.
            await asyncio.wait_for(asyncio.shield(self._future), timeout)
        except TimeoutError:
            return False
        return True


class FifoSemaphore:
    """A counting semaphore shared by threads and asyncio tasks.

    Waiters are served strictly in arrival order regardless of which world they came
    from: a permit is handed directly to the next waiter rather than returned to the
    pool, so an arriving caller cannot barge ahead of a queue.

    This type is concurrency safe.

    Args:
        size: The number of permits available.
    """

    __slots__ = ("_lock", "_size", "_used", "_waiters")

    def __init__(self, size: int) -> None:
        self._lock = threading.Lock()
        self._size = size
        self._used = 0
        self._waiters: collections.deque[_ThreadWaiter | _AsyncWaiter] = collections.deque()

    @property
    def used(self) -> int:
        """The number of permits currently held."""
        with self._lock:
            return self._used

    @property
    def waiters(self) -> int:
        """The number of callers blocked waiting for a permit."""
        with self._lock:
            return len(self._waiters)

    @property
    def size(self) -> int:
        """The number of permits the semaphore allows."""
        with self._lock:
            return self._size

    @property
    def is_full(self) -> bool:
        """Whether every permit is currently held."""
        with self._lock:
            return self._used >= self._size

    def resize(self, size: int) -> None:
        """Change how many permits the semaphore allows.

        Growing the semaphore hands the new permits straight to the longest-waiting
        callers. Shrinking never revokes a permit already held, so ``used`` may exceed
        ``size`` until those callers release.

        Args:
            size: The new number of permits.
        """
        with self._lock:
            if size > self._size:
                while self._used < size and self._waiters:
                    self._used += 1
                    self._waiters.popleft().claim()
            self._size = size

    def acquire(self, timeout: float | None = None) -> bool:
        """Acquire a permit, blocking the calling thread until one is available.

        Args:
            timeout: Seconds to wait, ``None`` to wait indefinitely, or ``0`` to take a
                permit only if one is immediately available.

        Returns:
            Whether a permit was acquired.
        """
        with self._lock:
            if self._available():
                self._used += 1
                return True
            if timeout == 0:
                return False
            waiter = _ThreadWaiter()
            self._waiters.append(waiter)
        try:
            granted = waiter.wait(timeout)
        except BaseException:
            self._unwind(waiter)
            raise
        return granted or not self._abandon(waiter)

    async def acquire_async(self, timeout: float | None = None) -> bool:
        """Acquire a permit, suspending the calling task until one is available.

        Args:
            timeout: Seconds to wait, ``None`` to wait indefinitely, or ``0`` to take a
                permit only if one is immediately available.

        Returns:
            Whether a permit was acquired.
        """
        with self._lock:
            if self._available():
                self._used += 1
                return True
            if timeout == 0:
                return False
            awaiter = _AsyncWaiter()
            self._waiters.append(awaiter)
        try:
            granted = await awaiter.wait(timeout)
        except BaseException:  # notably CancelledError: never leave a waiter behind
            self._unwind(awaiter)
            raise
        return granted or not self._abandon(awaiter)

    def release(self) -> None:
        """Release a permit, unblocking the longest-waiting caller if there is one.

        Raises:
            RuntimeError: If called without a matching successful acquire.
        """
        with self._lock:
            if self._used <= 0:
                raise RuntimeError("release() called without a matching acquire()")
            self._used -= 1
            # After a shrink `used` can exceed `size`; waking a waiter then would keep the
            # semaphore permanently over its limit, so capacity is rechecked here.
            if self._used < self._size and self._waiters:
                self._used += 1
                self._waiters.popleft().claim()

    def _available(self) -> bool:
        return self._used < self._size and not self._waiters

    def _abandon(self, waiter: _ThreadWaiter | _AsyncWaiter) -> bool:
        """Remove a waiter. Returns False if it was granted a permit in the meantime."""
        with self._lock:
            if waiter.granted:
                return False
            # ponytail: O(n) removal; Go uses an intrusive list. Swap in a linked list
            # only if a bulkhead is measured to queue deeply enough for this to show.
            self._waiters.remove(waiter)
            return True

    def _unwind(self, waiter: _ThreadWaiter | _AsyncWaiter) -> None:
        """Abandon a waiter whose caller is being torn down, releasing a late permit."""
        if not self._abandon(waiter):
            self.release()
