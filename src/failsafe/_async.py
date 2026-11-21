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
    is a :class:`BaseException` and is never treated as a failure or retried (D-5).

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

    async def call(self, fn: Callable[..., Awaitable[R]], /, *args: Any, **kwargs: Any) -> R:
        """Execute ``fn`` under the configured policies and return its result.

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
        raise AssertionError(f"unknown instruction {instruction!r}")

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
