"""The synchronous driver: services policy instructions by blocking the caller."""

from __future__ import annotations

from collections.abc import Callable, Generator
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

__all__ = ["Failsafe"]

R = TypeVar("R")


class Failsafe:
    """Executes functions under a composition of policies.

    Policies are composed around the function from left to right, so the leftmost is
    outermost and the rightmost handles the function's result first::

        Failsafe(fallback, retry, breaker)   # fallback(retry(breaker(fn)))

    The composition is built once, here, rather than per call. Construct one instance
    per backend and reuse it: stateful policies model the health of a backend, so
    constructing them per call site fragments their signal (D-4).

    The result type is inferred from the function passed to :meth:`call`, so one
    composition can guard calls returning different types.

    This type is concurrency safe, and shares its policies' state with any
    :class:`~failsafe.AsyncFailsafe` built from the same policy objects.

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

    def call(self, fn: Callable[..., R], /, *args: Any, **kwargs: Any) -> R:
        """Execute ``fn`` under the configured policies and return its result.

        Args:
            fn: The function to execute. Called once per attempt.
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
            return self._pump(self._chain(ex), fn, args, kwargs).unwrap()
        finally:
            token.var.reset(token)

    def _pump(
        self,
        gen: Generator[Instruction, Any, Outcome[Any]],
        fn: Callable[..., R],
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
                sent = self._service(instruction, fn, args, kwargs)
        finally:
            # Lets policies release permits from a finally block when the driver stops
            # pumping, which includes the caller being interrupted mid-execution.
            gen.close()

    def _service(
        self,
        instruction: Instruction,
        fn: Callable[..., R],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        if type(instruction) is Invoke:
            try:
                return Outcome(value=fn(*args, **kwargs))
            except Exception as exc:  # BaseException deliberately propagates
                return Outcome(exc=exc)
        if type(instruction) is Sleep:
            self._clock.sleep(instruction.seconds)
            return None
        if type(instruction) is Acquire:
            return instruction.semaphore.acquire(instruction.timeout)
        raise AssertionError(f"unknown instruction {instruction!r}")
