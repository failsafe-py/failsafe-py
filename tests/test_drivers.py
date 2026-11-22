"""Tests for the two drivers, and for the properties that justify sharing one core.

The load-bearing one is :func:`test_policy_state_is_shared_between_drivers`: if a policy
object tripped by a sync call were not seen tripped by an async one, the sans-io design
would buy nothing over duplicate implementations.
"""

from __future__ import annotations

import asyncio

import pytest
from conftest import aflaky, flaky

from failsafe import (
    AsyncFailsafe,
    Backoff,
    Bulkhead,
    BulkheadFull,
    CircuitBreaker,
    CircuitBreakerOpen,
    Failsafe,
    RetryPolicy,
    current_execution,
)
from failsafe._core import Acquire, Execution, FakeClock, compose


def test_policy_state_is_shared_between_drivers() -> None:
    breaker = CircuitBreaker(failure_threshold=1, clock=FakeClock())
    invoked = 0

    async def probe() -> str:
        nonlocal invoked
        invoked += 1
        return "ok"

    with pytest.raises(ConnectionError):
        Failsafe(breaker, clock=FakeClock()).call(flaky(9))
    assert breaker.is_open

    async def scenario() -> None:
        with pytest.raises(CircuitBreakerOpen):
            await AsyncFailsafe(breaker, clock=FakeClock()).call(probe)

    asyncio.run(scenario())
    assert invoked == 0, "the async side must see the breaker the sync side tripped"


def test_one_policy_object_drives_both_modes() -> None:
    retry = RetryPolicy(max_retries=3, delay=1.0)
    sync_clock = FakeClock()
    async_clock = FakeClock()

    assert Failsafe(retry, clock=sync_clock).call(flaky(2)) == "ok"

    async def scenario() -> str:
        result = await AsyncFailsafe(retry, clock=async_clock).call(aflaky(2))
        assert isinstance(result, str)
        return result

    assert asyncio.run(scenario()) == "ok"
    assert sync_clock.slept == async_clock.slept == [1.0, 1.0]


def test_backoff_state_is_per_execution_not_per_policy() -> None:
    """A shared policy must not carry one execution's backoff into the next."""
    policy = RetryPolicy(delay=Backoff(1.0, 60.0), max_retries=3)
    first, second = FakeClock(), FakeClock()

    Failsafe(policy, clock=first).call(flaky(3))
    Failsafe(policy, clock=second).call(flaky(3))

    assert first.slept == [1.0, 2.0, 4.0]
    assert second.slept == [1.0, 2.0, 4.0]


def test_async_driver_awaits_the_function() -> None:
    async def scenario() -> str:
        result = await AsyncFailsafe(clock=FakeClock()).call(aflaky(0, "value"))
        assert isinstance(result, str)
        return result

    assert asyncio.run(scenario()) == "value"


def test_cancelled_error_is_not_retried() -> None:
    """CancelledError is a BaseException: terminal, never a failure."""
    calls = 0

    async def cancelled() -> str:
        nonlocal calls
        calls += 1
        raise asyncio.CancelledError

    async def scenario() -> None:
        with pytest.raises(asyncio.CancelledError):
            await AsyncFailsafe(RetryPolicy(max_retries=5), clock=FakeClock()).call(cancelled)

    asyncio.run(scenario())
    assert calls == 1


def test_task_cancellation_propagates_and_releases_permits() -> None:
    """Cancelling mid-execution must unwind the generators, not strand a permit."""
    bulkhead = Bulkhead(1)
    started = asyncio.Event()

    async def hang() -> str:
        started.set()
        await asyncio.sleep(3600)
        return "never"

    async def scenario() -> None:
        fs = AsyncFailsafe(bulkhead, RetryPolicy(max_retries=5))
        task = asyncio.ensure_future(fs.call(hang))
        await started.wait()
        assert bulkhead.used == 1

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert bulkhead.used == 0

    asyncio.run(scenario())


def test_permit_is_released_when_the_driver_abandons_the_generator() -> None:
    """The driver may stop pumping at any instruction; finally must still run."""
    bulkhead = Bulkhead(1)
    ex = Execution(FakeClock())
    gen = compose((bulkhead, RetryPolicy(max_retries=1, delay=1.0)))(ex)

    instruction = gen.send(None)
    assert isinstance(instruction, Acquire)
    gen.send(instruction.semaphore.acquire(instruction.timeout))  # to the first Invoke
    assert bulkhead.used == 1

    gen.close()
    assert bulkhead.used == 0


def test_bulkhead_permit_is_acquired_per_attempt_when_inner_to_retry() -> None:
    """Composed inside a retry, a bulkhead re-acquires per attempt."""
    bulkhead = Bulkhead(1)
    fs = Failsafe(RetryPolicy(max_retries=2), bulkhead, clock=FakeClock())

    assert fs.call(flaky(2)) == "ok"
    assert bulkhead.used == 0


def test_blocked_permit_surfaces_as_a_policy_failure() -> None:
    bulkhead = Bulkhead(1)
    assert bulkhead.try_acquire_permit()
    fs = Failsafe(bulkhead, clock=FakeClock())

    with pytest.raises(BulkheadFull, match="bulkhead full"):
        fs.call(lambda: "never")


def test_current_execution_is_restored_after_nested_calls() -> None:
    inner = Failsafe(clock=FakeClock())
    outer = Failsafe(clock=FakeClock())
    seen: list[object] = []

    def nested() -> str:
        ex = current_execution()
        inner.call(lambda: None)
        seen.append(current_execution() is ex)
        return "ok"

    assert outer.call(nested) == "ok"
    assert seen == [True]
    assert current_execution() is None


def test_each_async_task_sees_its_own_execution() -> None:
    """contextvars are per-task, so interleaved calls do not share an Execution."""

    async def scenario() -> None:
        fs = AsyncFailsafe(RetryPolicy(max_retries=3))
        gate = asyncio.Event()

        async def probe(failures: int) -> list[int]:
            attempt = aflaky(failures)
            seen: list[int] = []
            await gate.wait()

            async def body() -> object:
                ex = current_execution()
                assert ex is not None
                seen.append(ex.attempts)
                return await attempt()

            await fs.call(body)
            return seen

        tasks = [asyncio.ensure_future(probe(n)) for n in (0, 2)]
        await asyncio.sleep(0)
        gate.set()
        assert await asyncio.gather(*tasks) == [[1], [1, 2, 3]]

    asyncio.run(scenario())


def test_decorator_preserves_the_signature() -> None:
    """One object, usable as a decorator, with ParamSpec fidelity."""
    import inspect

    payments = Failsafe(RetryPolicy(max_retries=3), clock=FakeClock())
    fn = flaky(2)

    @payments
    def charge(amount: int, *, currency: str = "EUR") -> str:
        """Charge the customer."""
        fn()
        return f"{amount} {currency}"

    assert charge(10, currency="USD") == "10 USD"
    assert charge.__doc__ == "Charge the customer."
    assert charge.__name__ == "charge"
    assert list(inspect.signature(charge).parameters) == ["amount", "currency"]


def test_async_decorator_preserves_the_signature() -> None:
    apayments = AsyncFailsafe(RetryPolicy(max_retries=3), clock=FakeClock())
    fn = aflaky(2)

    @apayments
    async def charge(amount: int) -> str:
        """Charge the customer."""
        await fn()
        return f"{amount}"

    assert asyncio.run(charge(10)) == "10"
    assert charge.__doc__ == "Charge the customer."


def test_decorated_functions_share_one_policy_set() -> None:
    """The fragmentation hazard: one object, one breaker, many call sites."""
    breaker = CircuitBreaker(failure_threshold=2, clock=FakeClock())
    payments = Failsafe(breaker, clock=FakeClock())

    @payments
    def charge() -> str:
        raise ConnectionError("down")

    @payments
    def refund() -> str:
        raise ConnectionError("down")

    with pytest.raises(ConnectionError):
        charge()
    with pytest.raises(ConnectionError):
        refund()
    assert breaker.is_open, "both call sites must feed the same breaker"
