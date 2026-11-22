"""Tests for the cross-mode FIFO semaphore.

Concurrency here is driven by explicit handoff events rather than sleeps: every
assertion is a positive observation of something that happened, not of something that
had not happened yet.
"""

from __future__ import annotations

import asyncio
import threading

import pytest

from failsafe._semaphore import FifoSemaphore


def test_permits_are_bounded() -> None:
    sem = FifoSemaphore(2)
    assert sem.acquire(timeout=0)
    assert sem.acquire(timeout=0)
    assert not sem.acquire(timeout=0)
    assert sem.used == 2
    sem.release()
    assert sem.acquire(timeout=0)


def test_release_without_acquire_is_an_error() -> None:
    with pytest.raises(RuntimeError, match="without a matching acquire"):
        FifoSemaphore(1).release()


def test_timed_out_waiter_leaves_no_permit_or_waiter_behind() -> None:
    sem = FifoSemaphore(1)
    assert sem.acquire(timeout=0)
    blocked = threading.Event()
    result: list[bool] = []

    def waiter() -> None:
        blocked.set()
        result.append(sem.acquire(timeout=0.05))

    thread = threading.Thread(target=waiter)
    thread.start()
    assert blocked.wait(1.0)
    thread.join(2.0)

    assert result == [False]
    assert sem.waiters == 0
    sem.release()
    assert sem.used == 0


def test_waiters_are_served_in_arrival_order() -> None:
    sem = FifoSemaphore(1)
    assert sem.acquire(timeout=0)
    served: list[int] = []
    served_lock = threading.Lock()
    threads = []

    for i in range(5):

        def worker(index: int = i) -> None:
            assert sem.acquire(timeout=5.0)
            with served_lock:
                served.append(index)
            sem.release()

        thread = threading.Thread(target=worker)
        thread.start()
        # Arrival order is enqueue order, so wait for this worker to be queued before
        # starting the next one.
        assert sem.await_waiters(i + 1), "worker never queued"
        threads.append(thread)

    sem.release()
    for thread in threads:
        thread.join(5.0)
    assert served == [0, 1, 2, 3, 4]


def test_one_instance_blocks_a_task_behind_threads() -> None:
    """Sync and async callers share permit state: the point of not splitting the type."""
    sem = FifoSemaphore(2)
    release_now = threading.Event()
    holding = [threading.Event(), threading.Event()]

    def holder(index: int) -> None:
        assert sem.acquire(timeout=5.0)
        holding[index].set()
        assert release_now.wait(5.0)
        sem.release()

    threads = [threading.Thread(target=holder, args=(i,)) for i in range(2)]
    for thread in threads:
        thread.start()
    for held in holding:
        assert held.wait(5.0), "holder never acquired its permit"
    assert sem.used == 2

    task_blocked = threading.Event()
    task_acquired = threading.Event()

    async def waiter() -> None:
        pending = asyncio.ensure_future(sem.acquire_async(timeout=5.0))
        while sem.waiters == 0:
            await asyncio.sleep(0)
        task_blocked.set()  # observed queued, not merely "not finished yet"
        assert await pending
        task_acquired.set()
        sem.release()

    task_thread = threading.Thread(target=lambda: asyncio.run(waiter()))
    task_thread.start()
    assert task_blocked.wait(5.0)
    assert not task_acquired.is_set()

    release_now.set()
    for thread in threads:
        thread.join(5.0)
    task_thread.join(5.0)

    assert task_acquired.is_set()
    assert sem.used == 0


def test_async_acquire_respects_capacity_and_timeout() -> None:
    async def scenario() -> None:
        sem = FifoSemaphore(1)
        assert await sem.acquire_async(timeout=0)
        assert not await sem.acquire_async(timeout=0.01)
        assert sem.waiters == 0
        sem.release()
        assert await sem.acquire_async(timeout=0)

    asyncio.run(scenario())


def test_cancelled_waiter_releases_a_permit_granted_in_the_race() -> None:
    """A cancellation that loses the race to a claim must not swallow the permit."""

    async def scenario() -> None:
        sem = FifoSemaphore(1)
        assert sem.acquire(timeout=0)
        task = asyncio.ensure_future(sem.acquire_async(timeout=5.0))
        while sem.waiters == 0:
            await asyncio.sleep(0)

        sem.release()  # hands the permit straight to the waiter
        task.cancel()  # ...which is cancelled before it can observe it
        with pytest.raises(asyncio.CancelledError):
            await task
        assert sem.used == 0
        assert sem.acquire(timeout=0)

    asyncio.run(scenario())


def test_size_and_is_full_report_capacity() -> None:
    sem = FifoSemaphore(2)

    assert sem.size == 2
    assert not sem.is_full

    assert sem.acquire(timeout=0)
    assert not sem.is_full
    assert sem.acquire(timeout=0)
    assert sem.is_full


def test_growing_hands_new_permits_to_waiters() -> None:
    """The adaptive limiter raises its limit by resizing; waiters must wake for it."""
    sem = FifoSemaphore(1)
    assert sem.acquire(timeout=0)
    granted: list[bool] = []

    def wait_for_permit() -> None:
        granted.append(sem.acquire(5.0))

    thread = threading.Thread(target=wait_for_permit, daemon=True)
    thread.start()
    assert sem.await_waiters(1), "waiter never queued"

    sem.resize(2)
    thread.join(5.0)

    assert granted == [True]
    assert sem.used == 2
    assert sem.waiters == 0


def test_shrinking_never_revokes_a_held_permit() -> None:
    sem = FifoSemaphore(3)
    for _ in range(3):
        assert sem.acquire(timeout=0)

    sem.resize(1)

    assert sem.size == 1
    assert sem.used == 3
    assert sem.is_full


def test_releasing_after_a_shrink_drains_to_the_new_size() -> None:
    """A waiter must not be handed a permit while the semaphore is still over its limit.

    Handing the permit straight over keeps ``used`` above ``size`` forever, which would
    pin the adaptive limiter above a limit it had just lowered.
    """
    sem = FifoSemaphore(3)
    for _ in range(3):
        assert sem.acquire(timeout=0)
    granted: list[bool] = []

    def wait_for_permit() -> None:
        granted.append(sem.acquire(5.0))

    thread = threading.Thread(target=wait_for_permit, daemon=True)
    thread.start()
    assert sem.await_waiters(1), "waiter never queued"

    sem.resize(1)
    sem.release()
    assert sem.used == 2
    sem.release()
    assert sem.used == 1
    assert granted == [], "a waiter was woken while the semaphore was over its limit"

    sem.release()
    thread.join(5.0)

    assert granted == [True]
    assert sem.used == 1
