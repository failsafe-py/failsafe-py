"""Budget semantics, ported from failsafe-go's budget corpus."""

from __future__ import annotations

import asyncio

import pytest

from failsafe import (
    AsyncFailsafe,
    Budget,
    BudgetExceeded,
    BudgetExceededEvent,
    Failsafe,
    HedgePolicy,
    RetryPolicy,
)
from failsafe._core import FakeClock


def saturate(budget: Budget, executions: int, extra: int) -> None:
    """Put the budget in a state as if other executions were already in flight.

    Go's tests reach into the budget's counters for this, since acquiring permits
    the normal way raises the denominator as well as the numerator.
    """
    budget._executions = executions
    budget._extra = extra


def test_rejects_invalid_configuration() -> None:
    with pytest.raises(ValueError, match="max_rate"):
        Budget(max_rate=0)
    with pytest.raises(ValueError, match="max_rate"):
        Budget(max_rate=1.5)
    with pytest.raises(ValueError, match="min_concurrency"):
        Budget(min_concurrency=-1)


def test_acquires_and_releases_permits() -> None:
    """Go TestBudget/should acquire and release permit."""
    budget = Budget(max_rate=0.5, min_concurrency=1)
    assert budget.rate == 0.0

    saturate(budget, executions=2, extra=1)
    assert budget.try_acquire_permit()
    assert not budget.try_acquire_permit()

    budget.release_permit()
    assert budget.try_acquire_permit()
    assert not budget.try_acquire_permit()


def test_min_concurrency_allows_a_permit_despite_the_rate() -> None:
    """Go TestBudget/when minConcurrency floor allows permit despite rate exceeded.

    A quiet service must still retry: with one execution in flight the rate is
    already at its ceiling, and only the floor lets the attempt through.
    """
    budget = Budget(max_rate=0.5, min_concurrency=2)
    saturate(budget, executions=1, extra=1)

    assert budget.try_acquire_permit()
    assert not budget.try_acquire_permit()


def test_rate_counts_extra_attempts_against_executions() -> None:
    budget = Budget()
    assert budget.rate == 0.0

    budget.record_execution()
    assert budget.rate == 0.0

    assert budget.try_acquire_permit()
    # The permit counts as an extra attempt and as an execution: 1 of 2.
    assert budget.rate == 0.5

    budget.release_permit()
    assert budget.rate == 0.0


def test_retry_proceeds_while_the_budget_allows_it() -> None:
    """Go TestBudget/when retries not exceeded."""
    budget = Budget(max_rate=0.5, min_concurrency=1)
    saturate(budget, executions=2, extra=1)
    calls: list[int] = []

    def flaky() -> str:
        calls.append(1)
        if len(calls) < 2:
            raise ValueError("first")
        return "ok"

    assert Failsafe(RetryPolicy(budget=budget), clock=FakeClock()).call(flaky) == "ok"
    assert len(calls) == 2


def test_retry_stops_when_the_budget_is_exceeded() -> None:
    """Go TestBudget/when retries exceeded: the retry is refused, not merely delayed."""
    budget = Budget(max_rate=0.5, min_concurrency=1)
    saturate(budget, executions=2, extra=2)
    events: list[BudgetExceededEvent] = []
    budget.on_exceeded.add(events.append)
    calls: list[int] = []

    def always_fails() -> str:
        calls.append(1)
        raise ValueError("boom")

    with pytest.raises(BudgetExceeded) as caught:
        Failsafe(RetryPolicy(budget=budget), clock=FakeClock()).call(always_fails)

    assert len(calls) == 1
    assert isinstance(caught.value.__cause__, ValueError)
    assert caught.value.max_rate == 0.5
    assert [event.kind for event in events] == ["retry"]


def test_the_budget_is_released_after_an_execution() -> None:
    budget = Budget()
    failsafe = Failsafe(RetryPolicy(budget=budget), clock=FakeClock())

    failsafe.call(lambda: "ok")
    assert budget.rate == 0.0

    calls: list[int] = []

    def flaky() -> str:
        calls.append(1)
        if len(calls) < 3:
            raise ValueError("retry me")
        return "ok"

    assert failsafe.call(flaky) == "ok"
    assert budget.rate == 0.0


def test_a_refused_retry_still_releases_the_budget() -> None:
    budget = Budget(max_rate=0.5, min_concurrency=1)
    saturate(budget, executions=2, extra=2)

    with pytest.raises(BudgetExceeded):
        Failsafe(RetryPolicy(budget=budget), clock=FakeClock()).call(_raise)

    # The execution the policy recorded is gone; the injected state remains.
    assert budget._executions == 2


def test_one_budget_shared_across_policies_sees_their_sum() -> None:
    """The reason to share an instance: two policies, one dependency, one ceiling."""
    budget = Budget(max_rate=0.5, min_concurrency=1)
    saturate(budget, executions=2, extra=2)
    first = Failsafe(RetryPolicy(budget=budget), clock=FakeClock())
    second = Failsafe(RetryPolicy(budget=budget), clock=FakeClock())

    for failsafe in (first, second):
        with pytest.raises(BudgetExceeded):
            failsafe.call(_raise)


def test_hedging_stops_when_the_budget_is_exceeded() -> None:
    """Go TestBudget/when hedges exceeded.

    The initial attempt is never budgeted, so it still produces a result; only the
    hedge is refused.
    """
    budget = Budget(max_rate=0.5, min_concurrency=1)
    saturate(budget, executions=2, extra=2)
    events: list[BudgetExceededEvent] = []
    budget.on_exceeded.add(events.append)
    started: list[int] = []

    async def slow() -> str:
        index = len(started)
        started.append(index)
        await asyncio.sleep(0.01)
        return f"attempt{index}"

    policy = HedgePolicy(0.001, budget=budget)
    result = asyncio.run(AsyncFailsafe(policy, clock=FakeClock()).call(slow))

    assert result == "attempt0"
    assert len(started) == 1
    assert [event.kind for event in events] == ["hedge"]


def test_hedging_proceeds_while_the_budget_allows_it() -> None:
    """Go TestBudget/when hedges not exceeded."""
    budget = Budget(max_rate=0.5, min_concurrency=1)
    saturate(budget, executions=2, extra=1)
    started: list[int] = []

    async def slow_then_fast() -> str:
        index = len(started)
        started.append(index)
        if index == 0:
            await asyncio.sleep(10)
        return f"attempt{index}"

    policy = HedgePolicy(0.01, budget=budget)
    result = asyncio.run(AsyncFailsafe(policy, clock=FakeClock()).call(slow_then_fast))

    assert result == "attempt1"
    assert len(started) == 2
    # The hedge released its permit, leaving only the injected state.
    assert budget._extra == 1


def _raise() -> str:
    raise ValueError("boom")
