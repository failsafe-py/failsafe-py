"""The synchronous driver: services policy instructions by blocking the caller."""

from __future__ import annotations

import contextvars
import functools
from collections.abc import Callable, Generator
from concurrent.futures import ThreadPoolExecutor, wait
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

__all__ = ["Failsafe"]

R = TypeVar("R")
P = ParamSpec("P")


class Failsafe:
    """Executes functions under a composition of policies.

    Policies are composed around the function from left to right, so the leftmost is
    outermost and the rightmost handles the function's result first:

    ```python
    Failsafe(fallback, retry, breaker)   # fallback(retry(breaker(fn)))
    ```

    The composition is built once, here, rather than per call. Stateful policies
    accumulate state across executions, so a reused instance measures whatever its
    callers have in common — one per backend tracks that backend, one per operation
    tracks something narrower.

    The result type is inferred from the function passed to `call`, so one
    composition can guard calls returning different types.

    This type is concurrency safe, and shares its policies' state with any
    [`AsyncFailsafe`][failsafe.AsyncFailsafe] built from the same policy objects.

    Args:
        *policies: Policies, outermost first.
        clock: Custom time source for delays and deadlines (e.g. for testing).
    """

    __slots__ = ("_chain", "_clock", "_pool", "policies")

    def __init__(self, *policies: Policy, clock: Clock = SYSTEM_CLOCK) -> None:
        self.policies = policies
        """The configured policies, outermost first."""
        self._clock = clock
        self._chain = compose(policies)
        self._pool: ThreadPoolExecutor | None = None

    def call(self, fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
        """Execute ``fn`` under the configured policies and return its result.

        The result type comes from ``fn``, and the arguments are checked against its
        signature, so one composition can guard calls returning different types
        without giving up either check.

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
        ex.args = args
        ex.kwargs = kwargs
        token = set_current_execution(ex)
        try:
            return self._pump(self._chain(ex), ex, fn, args, kwargs).unwrap()
        finally:
            token.var.reset(token)

    def __call__(self, fn: Callable[P, R]) -> Callable[P, R]:
        """Decorate ``fn`` so every call runs under these policies.

        The decorated function keeps its signature, name and docstring.

        Construct the composition once and reuse it. Writing
        ``@Failsafe(RetryPolicy(), CircuitBreaker())`` on twenty functions builds twenty
        independent breakers, each seeing a twentieth of the traffic, which no type
        checker can catch.

        Example:
            ```python
            payments = Failsafe(retry, breaker)
            @payments
            def charge(amount): ...
            ```
        """

        @functools.wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return self.call(fn, *args, **kwargs)

        return wrapper

    def _pump(
        self,
        gen: Generator[Instruction, Any, Outcome[Any]],
        ex: Execution,
        fn: Callable[..., R],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        deadline: float | None = None,
    ) -> Outcome[R]:
        """Drive a policy generator to completion, servicing what it yields.

        ``deadline`` makes the pump cooperative: an enclosing `Race` stops it at
        the next instruction boundary once the time is past, which is as close to
        interruption as sync Python gets without a thread.
        """
        sent: Any = None
        try:
            while True:
                if deadline is not None and self._clock.now() >= deadline:
                    return Outcome(value=None)  # abandoned; Race reports the timeout
                try:
                    instruction = gen.send(sent)
                except StopIteration as stop:
                    outcome: Outcome[R] = stop.value
                    return outcome
                sent = self._service(instruction, ex, fn, args, kwargs, deadline)
        finally:
            # Lets policies release permits from a finally block when the driver stops
            # pumping, which includes the caller being interrupted mid-execution.
            gen.close()

    def _service(
        self,
        instruction: Instruction,
        ex: Execution,
        fn: Callable[..., R],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        deadline: float | None,
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
        if type(instruction) is Race:
            return self._race(instruction, ex, fn, args, kwargs, deadline)
        if type(instruction) is Hedge:
            raise NotImplementedError(
                "HedgePolicy requires AsyncFailsafe: cancelling a losing attempt is "
                "impossible for a synchronous call, and leaving it running would be a "
                "weaker policy than the name promises"
            )
        raise AssertionError(f"unknown instruction {instruction!r}")

    def _race(
        self,
        race: Race,
        ex: Execution,
        fn: Callable[..., R],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        outer_deadline: float | None,
    ) -> Outcome[Any] | None:
        """Run an inner step under a deadline, returning ``None`` if it elapsed."""
        deadline = self._clock.now() + race.seconds
        if outer_deadline is not None:
            deadline = min(deadline, outer_deadline)
        previous_deadline, ex.deadline = ex.deadline, deadline
        try:
            if race.interrupt:
                return self._race_interrupting(race, ex, fn, args, kwargs)
            outcome = self._pump(race.inner(ex), ex, fn, args, kwargs, deadline)
            if self._clock.now() >= deadline:
                return None
            return outcome
        finally:
            ex.deadline = previous_deadline

    def _race_interrupting(
        self,
        race: Race,
        ex: Execution,
        fn: Callable[..., R],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Outcome[Any] | None:
        """Run an inner step on a worker thread, freeing the caller when time is up.

        The abandoned thread keeps running to completion: Python cannot kill a thread,
        and killing one mid-call would corrupt whatever it holds. This is the documented
        cost of ``interrupt=True``.
        """
        if self._pool is None:
            # ponytail: one unbounded pool per Failsafe, created on first interrupting
            # timeout. Bound it if a caller is measured to exhaust threads.
            self._pool = ThreadPoolExecutor(thread_name_prefix="failsafe-timeout")
        context = contextvars.copy_context()
        gen = race.inner(ex)
        future = self._pool.submit(context.run, self._pump, gen, ex, fn, args, kwargs)
        # Wall time, not the clock seam: a worker thread is real concurrency, and a
        # FakeClock cannot make another thread finish.
        done, _ = wait([future], timeout=race.seconds)
        if not done:
            return None
        outcome: Outcome[Any] = future.result()
        return outcome
