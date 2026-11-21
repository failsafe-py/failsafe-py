"""Regressions ported from failsafe-go's `test/issues`.

Only the issues that apply to phase 1 are here. The rest are out of scope by design:

- issue65, issue73: HTTP body rewind and streaming (phase 3).
- issue100: ``ComposeAny``/``WithAny``, which generics erasure deletes outright.
- issue122: gRPC interceptors carrying context values (not ported).
- issue134: child-context cancellation after completion; no analogue, since sync has no
  cancellation and async uses task cancellation.
- issue141: hedge budgets (phase 6).
"""

from __future__ import annotations

import asyncio
import threading

import pytest
from conftest import flaky

from failsafe import (
    AsyncFailsafe,
    CircuitBreaker,
    Failsafe,
    RetriesExceeded,
    RetryPolicy,
)
from failsafe._core import FakeClock


def test_issue136_concurrent_calls_through_one_executor() -> None:
    """Go TestIssue136: concurrent calls must not race on a shared executor.

    Go's bug was a shared ``Executor`` mutating per-call state. Our equivalent hazard is
    the composed chain and the policy objects, which every caller shares.
    """
    breaker = CircuitBreaker(failure_threshold=100)
    fs = Failsafe(RetryPolicy(max_retries=2), breaker)
    results: list[str] = []
    errors: list[BaseException] = []
    lock = threading.Lock()
    start = threading.Barrier(20)

    def worker(index: int) -> None:
        start.wait(5.0)
        try:
            result = fs.call(lambda: f"ok-{index}")
            with lock:
                results.append(result)
        except BaseException as exc:
            with lock:
                errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10.0)

    assert errors == []
    assert sorted(results) == sorted(f"ok-{i}" for i in range(20))
    assert breaker.is_closed


def test_concurrent_executions_do_not_share_retry_state() -> None:
    """One policy object, many simultaneous executions, each with its own attempt count.

    Each worker needs a different number of retries. If the policy leaked its failure
    count between executions, the workers needing the most retries would exhaust a
    budget consumed by their neighbours.
    """
    workers = 10
    policy = RetryPolicy(max_retries=workers)
    fs = Failsafe(policy)
    attempts: dict[int, int] = {}
    lock = threading.Lock()
    start = threading.Barrier(workers)

    def worker(failures: int) -> None:
        fn = flaky(failures)
        start.wait(5.0)

        def counting() -> str:
            with lock:
                attempts[failures] = attempts.get(failures, 0) + 1
            return fn()

        assert fs.call(counting) == "ok"

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10.0)

    assert attempts == {i: i + 1 for i in range(workers)}, (
        "each execution must retry exactly as often as its own failures require"
    )


def test_a_breaker_shared_across_threads_opens_once() -> None:
    """Every caller must see one consistent state, not a torn one."""
    breaker = CircuitBreaker(failure_threshold=3)
    transitions: list[str] = []
    lock = threading.Lock()
    breaker.on_open.add(lambda e: transitions.append("open"))
    start = threading.Barrier(16)

    def worker() -> None:
        start.wait(5.0)
        with pytest.raises(Exception, match=r"boom|circuit breaker open"):
            Failsafe(breaker).call(flaky(99))

    threads = [threading.Thread(target=worker) for _ in range(16)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10.0)

    assert breaker.is_open
    with lock:
        assert transitions == ["open"], "the breaker must open exactly once"


def test_sync_and_async_callers_share_one_breaker() -> None:
    """The sans-io core's decisive property, under real concurrency rather than in sequence."""
    breaker = CircuitBreaker(failure_threshold=1, delay=60.0, clock=FakeClock())

    with pytest.raises(ConnectionError):
        Failsafe(breaker).call(flaky(9))

    rejected = 0

    async def scenario() -> None:
        nonlocal rejected

        async def probe() -> str:
            return "ok"

        afs = AsyncFailsafe(breaker)
        results = await asyncio.gather(*(afs.call(probe) for _ in range(8)), return_exceptions=True)
        rejected = sum(isinstance(r, BaseException) for r in results)

    asyncio.run(scenario())
    assert rejected == 8, "every async caller must see the breaker the sync call tripped"


def test_retries_exceeded_chains_the_original_traceback() -> None:
    """Go's ExceededError.Unwrap; ours is __cause__, so `raise from` context survives."""
    with pytest.raises(RetriesExceeded) as info:
        Failsafe(RetryPolicy(max_retries=1)).call(flaky(9))

    assert isinstance(info.value.__cause__, ConnectionError)
    assert info.value.__cause__ is info.value.last_exception
