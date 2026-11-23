from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from failsafe import (
    AsyncFailsafe,
    ExecutionEvent,
    Failsafe,
    HedgePolicy,
    RetryPolicy,
    current_execution,
)
from failsafe._core import FakeClock

HANGS = 10.0
"""Long enough that an attempt sleeping on it only ends by cancellation."""


def run(policy: HedgePolicy, fn: Callable[..., Awaitable[Any]], *args: Any) -> Any:
    """Execute ``fn`` under ``policy`` on a fake clock."""
    failsafe = AsyncFailsafe(policy, clock=FakeClock())
    return asyncio.run(failsafe.call(fn, *args))


def test_rejects_invalid_configuration() -> None:
    with pytest.raises(ValueError, match="exactly one of delay or quantile"):
        HedgePolicy()
    with pytest.raises(ValueError, match="exactly one of delay or quantile"):
        HedgePolicy(0.1, quantile=0.95)
    with pytest.raises(ValueError, match="max_hedges"):
        HedgePolicy(0.1, max_hedges=0)
    with pytest.raises(ValueError, match="quantile must be between"):
        HedgePolicy(quantile=1.0)
    with pytest.raises(ValueError, match="quantile_age"):
        HedgePolicy(quantile=0.95, quantile_age=0)
    with pytest.raises(ValueError, match="warmup"):
        HedgePolicy(quantile=0.95, warmup=0)
    with pytest.raises(ValueError, match="delay must not be negative"):
        HedgePolicy(-1.0)


def test_sync_rejects_hedging() -> None:
    """Hedging under a sync call would leave the losers running to completion."""
    with pytest.raises(NotImplementedError, match="AsyncFailsafe"):
        Failsafe(HedgePolicy(0.1)).call(lambda: "never")


def test_no_hedge_when_the_delay_is_not_exceeded() -> None:
    started: list[int] = []

    async def fast() -> str:
        started.append(1)
        return "ok"

    assert run(HedgePolicy(1.0), fast) == "ok"
    assert len(started) == 1


def test_hedges_when_the_delay_is_exceeded() -> None:
    started: list[int] = []

    async def slow_then_fast() -> str:
        index = len(started)
        started.append(index)
        if index == 0:
            await asyncio.sleep(HANGS)
        return f"attempt{index}"

    assert run(HedgePolicy(0.01), slow_then_fast) == "attempt1"
    assert len(started) == 2


def test_max_hedges_bounds_the_attempts() -> None:
    started: list[int] = []

    async def slow() -> str:
        index = len(started)
        started.append(index)
        if index < 2:
            await asyncio.sleep(HANGS)
        return f"attempt{index}"

    assert run(HedgePolicy(0.01, max_hedges=2), slow) == "attempt2"
    assert len(started) == 3


def test_the_first_attempt_to_finish_wins() -> None:
    """The hedge exists for this: a stalled attempt does not decide the result."""
    started: list[int] = []

    async def first_hangs() -> str:
        index = len(started)
        started.append(index)
        if index == 0:
            await asyncio.sleep(HANGS)
        return "hedge"

    assert run(HedgePolicy(0.01), first_hangs) == "hedge"


def test_losing_attempts_are_cancelled() -> None:
    """The capability that makes this async-only: the loser's I/O actually stops."""
    cancelled: list[int] = []
    started: list[int] = []

    async def track() -> str:
        index = len(started)
        started.append(index)
        try:
            if index == 0:
                await asyncio.sleep(HANGS)
            return f"attempt{index}"
        except asyncio.CancelledError:
            cancelled.append(index)
            raise

    async def main() -> str:
        failsafe = AsyncFailsafe(HedgePolicy(0.01), clock=FakeClock())
        result = await failsafe.call(track)
        await asyncio.sleep(0)
        return result

    assert asyncio.run(main()) == "attempt1"
    assert cancelled == [0], "the stalled attempt must be cancelled, not abandoned"


def test_hedges_count_as_attempts_and_hedges() -> None:
    seen: list[tuple[int, int, int, bool]] = []

    async def record() -> str:
        execution = current_execution()
        assert execution is not None
        seen.append((execution.attempts, execution.retries, execution.hedges, execution.is_hedge))
        await asyncio.sleep(HANGS)
        return "never"

    async def main() -> None:
        failsafe = AsyncFailsafe(HedgePolicy(0.01, max_hedges=2), clock=FakeClock())
        task = asyncio.create_task(failsafe.call(record))
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(main())
    assert seen == [(1, 0, 0, False), (2, 0, 1, True), (3, 0, 2, True)]


def test_a_hedge_is_not_a_retry() -> None:
    """Concurrent attempts are not retries, so an outer retry's budget is untouched."""
    seen: list[int] = []

    async def record() -> str:
        execution = current_execution()
        assert execution is not None
        seen.append(execution.retries)
        if not execution.is_hedge:
            await asyncio.sleep(HANGS)
        return "ok"

    assert run(HedgePolicy(0.01), record) == "ok"
    assert seen == [0, 0]


def test_backup_executions_start_immediately() -> None:
    started: list[int] = []

    async def numbered() -> int:
        index = len(started)
        started.append(index)
        await asyncio.sleep(0)
        return index

    policy = HedgePolicy(0.0, max_hedges=2, cancel_on=2)
    assert run(policy, numbered) == 2
    assert len(started) == 3


def test_cancel_on_waits_for_a_matching_result() -> None:
    started: list[int] = []

    async def numbered() -> int:
        index = len(started)
        started.append(index)
        await asyncio.sleep(0)
        return index

    policy = HedgePolicy(0.0, max_hedges=4, cancel_on=3)
    assert run(policy, numbered) == 3
    assert len(started) == 4, "attempts stop as soon as one matches, before the cap"


def test_the_last_result_is_returned_when_none_is_acceptable() -> None:
    started: list[int] = []

    async def unmatched() -> int:
        started.append(1)
        return 99

    policy = HedgePolicy(0.0, max_hedges=2, cancel_on=1)
    assert run(policy, unmatched) == 99
    assert len(started) == 3


def test_a_delay_of_none_stops_hedging() -> None:
    started: list[int] = []

    async def slow() -> str:
        started.append(1)
        await asyncio.sleep(0)
        return "only"

    policy = HedgePolicy(lambda _ex: None, max_hedges=3)
    assert run(policy, slow) == "only"
    assert len(started) == 1


def test_a_delay_callable_sees_the_execution() -> None:
    seen: list[int] = []

    def delay(execution: object) -> float:
        seen.append(getattr(execution, "attempts", -1))
        return 0.01

    started: list[int] = []

    async def slow_then_fast() -> str:
        index = len(started)
        started.append(index)
        if index == 0:
            await asyncio.sleep(HANGS)
        return f"attempt{index}"

    assert run(HedgePolicy(delay), slow_then_fast) == "attempt1"
    assert seen, "the delay callable must be consulted"


def test_a_failure_is_an_acceptable_outcome_by_default() -> None:
    """Without cancel_on the first outcome wins, exception or not."""
    started: list[int] = []

    async def fails() -> str:
        started.append(1)
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        run(HedgePolicy(0.01), fails)
    assert len(started) == 1


def test_on_hedge_fires_for_each_hedge() -> None:
    events: list[ExecutionEvent] = []
    started: list[int] = []

    async def slow() -> str:
        index = len(started)
        started.append(index)
        if index < 2:
            await asyncio.sleep(HANGS)
        return "ok"

    policy = HedgePolicy(0.01, max_hedges=2, on_hedge=events.append)
    assert run(policy, slow) == "ok"
    assert len(events) == 2, "one event per hedge, not per attempt"


def test_the_quantile_times_each_attempt_not_the_whole_execution() -> None:
    clock = FakeClock()
    policy = HedgePolicy(quantile=0.95, quantile_age=30, warmup=1)
    failsafe = AsyncFailsafe(policy, clock=clock)
    warmup_seconds = 0.5
    attempt_seconds = 0.001

    async def warm() -> str:
        clock.advance(warmup_seconds)
        return "ok"

    async def hedged() -> str:
        execution = current_execution()
        assert execution is not None
        if not execution.is_hedge:
            # The primary stalls, and the clock runs on while it does, so the
            # execution spans far longer than the attempt that wins.
            clock.advance(warmup_seconds)
            await asyncio.sleep(HANGS)
        clock.advance(attempt_seconds)
        return "ok"

    async def main() -> None:
        # One sample ends the warmup, so the next execution may hedge.
        assert await failsafe.call(warm) == "ok"
        assert await failsafe.call(hedged) == "ok"

    asyncio.run(main())

    assert policy._quantile is not None
    assert policy._quantile.count == 2, "one sample per accepted attempt"
    # Recording the execution's elapsed time would feed back a sample of at least
    # warmup_seconds, pushing the estimate above the first sample rather than below.
    assert policy._quantile.value < warmup_seconds, (
        "the sample must be the winning attempt's duration, not the execution's"
    )


def test_quantile_does_not_hedge_during_warmup() -> None:
    started: list[int] = []

    async def steady() -> str:
        started.append(1)
        await asyncio.sleep(0)
        return "ok"

    policy = HedgePolicy(quantile=0.95, quantile_age=30, warmup=20)
    failsafe = AsyncFailsafe(policy, clock=FakeClock())

    async def main() -> None:
        for _ in range(20):
            assert await failsafe.call(steady) == "ok"

    asyncio.run(main())
    assert len(started) == 20, "no hedges before the warmup is over"


def test_quantile_hedges_a_slow_execution_after_warmup() -> None:
    clock = FakeClock()
    policy = HedgePolicy(quantile=0.95, quantile_age=30, warmup=5)
    failsafe = AsyncFailsafe(policy, clock=clock)
    fast_calls: list[int] = []

    async def fast() -> str:
        fast_calls.append(1)
        clock.advance(0.005)
        return "ok"

    slow_started: list[int] = []

    async def slow_then_hedge() -> str:
        execution = current_execution()
        assert execution is not None
        slow_started.append(1)
        if not execution.is_hedge:
            await asyncio.sleep(HANGS)
        return "hedged"

    async def main() -> str:
        for _ in range(10):
            await failsafe.call(fast)
        return await failsafe.call(slow_then_hedge)

    assert asyncio.run(main()) == "hedged"
    assert len(slow_started) == 2, "the slow execution must be hedged once warmed up"


def test_composes_inside_a_retry() -> None:
    """A retry outside a hedge retries the whole hedged execution."""
    attempts: list[int] = []

    async def fails_then_succeeds() -> str:
        index = len(attempts)
        attempts.append(index)
        if index == 0:
            raise ValueError("first")
        return "ok"

    failsafe = AsyncFailsafe(RetryPolicy(max_retries=2), HedgePolicy(1.0), clock=FakeClock())
    assert asyncio.run(failsafe.call(fails_then_succeeds)) == "ok"
    assert len(attempts) == 2
