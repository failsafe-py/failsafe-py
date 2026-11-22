"""The asyncio driver: services policy instructions by suspending the caller."""

from __future__ import annotations

import asyncio
import functools
from collections.abc import Awaitable, Callable, Coroutine, Generator
from typing import Any, ParamSpec, TypeVar

from ._core import (
    SYSTEM_CLOCK,
    Acquire,
    Clock,
    Execution,
    Hedge,
    Instruction,
    Invoke,
    Outcome,
    Policy,
    Race,
    Sleep,
    compose,
    set_current_execution,
)

__all__ = ["AsyncFailsafe"]

R = TypeVar("R")
P = ParamSpec("P")


class AsyncFailsafe:
    """Executes coroutine functions under a composition of policies.

    The async counterpart of :class:`~failsafe.Failsafe`, with the same composition
    order and the same policy objects. Policy state is shared between the two: a
    circuit breaker tripped by a sync call is seen open by an async one.

    Cancellation is ordinary :mod:`asyncio` cancellation. :exc:`asyncio.CancelledError`
    is a :class:`BaseException` and is never treated as a failure or retried.

    The result type is inferred from the function passed to :meth:`call`.

    This type is concurrency safe.

    Args:
        *policies: Policies, outermost first.
        clock: The time source for delays and deadlines.
    """

    __slots__ = ("_chain", "_clock", "policies")

    def __init__(self, *policies: Policy, clock: Clock = SYSTEM_CLOCK) -> None:
        self.policies = policies
        """The configured policies, outermost first."""
        self._clock = clock
        self._chain = compose(policies)

    async def call(self, fn: Callable[P, Awaitable[R]], /, *args: P.args, **kwargs: P.kwargs) -> R:
        """Execute ``fn`` under the configured policies and return its result.

        The result type comes from ``fn``, and the arguments are checked against its
        signature, so one composition can guard calls returning different types
        without giving up either check.

        Args:
            fn: The coroutine function to execute. Awaited once per attempt.
            *args: Positional arguments for ``fn``.
            **kwargs: Keyword arguments for ``fn``.

        Returns:
            The result of the final attempt.

        Raises:
            Exception: Whatever the final attempt or the policies produce.
        """
        ex = Execution(self._clock)
        ex.args = args
        ex.kwargs = kwargs
        token = set_current_execution(ex)
        try:
            outcome = await self._pump(self._chain(ex), ex, fn, args, kwargs)
            return outcome.unwrap()
        finally:
            token.var.reset(token)

    def __call__(self, fn: Callable[P, Awaitable[R]]) -> Callable[P, Coroutine[Any, Any, R]]:
        """Decorate a coroutine function so every call runs under these policies.

        The decorated function keeps its signature, name and docstring.

        Example:
            >>> payments = AsyncFailsafe(retry, breaker)
            >>> @payments
            ... async def charge(amount): ...
        """

        @functools.wraps(fn)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return await self.call(fn, *args, **kwargs)

        return wrapper

    async def _pump(
        self,
        gen: Generator[Instruction, Any, Outcome[Any]],
        ex: Execution,
        fn: Callable[..., Awaitable[R]],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Outcome[R]:
        """Drive a policy generator to completion, servicing what it yields."""
        sent: Any = None
        try:
            while True:
                try:
                    instruction = gen.send(sent)
                except StopIteration as stop:
                    outcome: Outcome[R] = stop.value
                    return outcome
                sent = await self._service(instruction, ex, fn, args, kwargs)
        finally:
            # Lets policies release permits from a finally block when the driver stops
            # pumping, which includes the task being cancelled mid-execution.
            gen.close()

    async def _service(
        self,
        instruction: Instruction,
        ex: Execution,
        fn: Callable[..., Awaitable[R]],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        if type(instruction) is Invoke:
            try:
                return Outcome(value=await fn(*args, **kwargs))
            except Exception as exc:  # CancelledError is a BaseException: propagates
                return Outcome(exc=exc)
        if type(instruction) is Sleep:
            await self._clock.sleep_async(instruction.seconds)
            return None
        if type(instruction) is Acquire:
            return await instruction.semaphore.acquire_async(instruction.timeout)
        if type(instruction) is Race:
            return await self._race(instruction, ex, fn, args, kwargs)
        if type(instruction) is Hedge:
            return await self._hedge(instruction, ex, fn, args, kwargs)
        raise AssertionError(f"unknown instruction {instruction!r}")

    async def _hedge(
        self,
        hedge: Hedge,
        ex: Execution,
        fn: Callable[..., Awaitable[R]],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Outcome[Any]:
        """Run concurrent attempts, returning the first the policy accepts.

        Attempts start one at a time: each is given its delay to finish before the
        next joins it. Losers are cancelled once a winner is accepted, so their I/O
        stops rather than running on in the background. When no attempt is ever
        accepted, the last one to finish is returned.
        """
        control = hedge.control
        pending: set[asyncio.Task[tuple[Outcome[Any], Execution]]] = set()
        hedged: set[asyncio.Task[tuple[Outcome[Any], Execution]]] = set()
        last: Outcome[Any] | None = None
        started = 0
        more = True
        try:
            while True:
                if more:
                    attempt_ex = ex if started == 0 else ex.copy_for_hedge()
                    started += 1
                    task = asyncio.ensure_future(
                        self._hedged_attempt(hedge.inner, attempt_ex, fn, args, kwargs)
                    )
                    pending.add(task)
                    if attempt_ex.is_hedge:
                        hedged.add(task)
                    delay = control.hedge_delay(ex)
                    more = delay is not None and control.start_hedge(ex)
                else:
                    delay = None

                timer = (
                    asyncio.ensure_future(self._clock.sleep_async(delay))
                    if more and delay is not None
                    else None
                )
                try:
                    while pending:
                        waits: set[asyncio.Future[Any]] = set(pending)
                        if timer is not None:
                            waits.add(timer)
                        await asyncio.wait(waits, return_when=asyncio.FIRST_COMPLETED)
                        for task in [t for t in pending if t.done()]:
                            pending.discard(task)
                            if task in hedged:
                                hedged.discard(task)
                                control.finish_hedge()
                            last, finished_ex = task.result()
                            accepted = control.accepts(last)
                            # Go times each attempt inside its own goroutine, and only
                            # an acceptable outcome teaches the estimate anything.
                            if accepted:
                                control.record_duration(finished_ex)
                                return self._won(ex, last)
                        if timer is not None and timer.done():
                            break
                finally:
                    if timer is not None and not timer.done():
                        timer.cancel()
                if not pending and not more:
                    # Nothing left to wait for and nothing acceptable arrived.
                    assert last is not None
                    return self._won(ex, last)
        finally:
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            for _ in hedged:
                control.finish_hedge()

    async def _hedged_attempt(
        self,
        inner: Any,
        attempt_ex: Execution,
        fn: Callable[..., Awaitable[R]],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> tuple[Outcome[Any], Execution]:
        """Pump one hedged attempt, with its own execution visible to user code.

        Returns the attempt's execution alongside its outcome so the policy can time
        that attempt rather than the hedged execution as a whole.
        """
        set_current_execution(attempt_ex)
        outcome = await self._pump(inner(attempt_ex), attempt_ex, fn, args, kwargs)
        return outcome, attempt_ex

    @staticmethod
    def _won(ex: Execution, outcome: Outcome[Any]) -> Outcome[Any]:
        """Record the winning attempt's outcome on the parent execution."""
        ex.last_result = outcome.value
        ex.last_exception = outcome.exc
        return outcome

    async def _race(
        self,
        race: Race,
        ex: Execution,
        fn: Callable[..., Awaitable[R]],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Outcome[Any] | None:
        """Run an inner step under a deadline, returning ``None`` if it elapsed.

        :func:`asyncio.timeout` is the mechanism, not something to reimplement: it
        interrupts whatever is awaiting inside the scope and exposes the absolute
        deadline, which user code reads through ``Execution.remaining()``.
        """
        previous_deadline = ex.deadline
        try:
            # Relative seconds: asyncio.timeout_at is the absolute-deadline variant.
            async with asyncio.timeout(race.seconds) as scope:
                ex.deadline = self._deadline_from(scope.when())
                return await self._pump(race.inner(ex), ex, fn, args, kwargs)
        except TimeoutError:
            return None
        finally:
            ex.deadline = previous_deadline

    def _deadline_from(self, when: float | None) -> float | None:
        """Translate a loop-clock deadline onto the configured clock."""
        if when is None:
            return None
        return self._clock.now() + (when - asyncio.get_running_loop().time())
