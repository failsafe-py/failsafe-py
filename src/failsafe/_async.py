"""The asyncio driver: services policy instructions by suspending the caller."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Generator
from typing import Any, TypeVar

from ._core import (
    SYSTEM_CLOCK,
    Acquire,
    Clock,
    Execution,
    Instruction,
    Invoke,
    Outcome,
    Policy,
    Sleep,
    compose,
    set_current_execution,
)

__all__ = ["AsyncFailsafe"]

R = TypeVar("R")


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
            outcome = await self._pump(self._chain(ex), fn, args, kwargs)
            return outcome.unwrap()
        finally:
            token.var.reset(token)

    async def _pump(
        self,
        gen: Generator[Instruction, Any, Outcome[Any]],
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
                sent = await self._service(instruction, fn, args, kwargs)
        finally:
            # Lets policies release permits from a finally block when the driver stops
            # pumping, which includes the task being cancelled mid-execution.
            gen.close()

    async def _service(
        self,
        instruction: Instruction,
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
        raise AssertionError(f"unknown instruction {instruction!r}")
