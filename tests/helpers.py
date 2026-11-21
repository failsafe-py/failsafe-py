"""Shared test helpers.

``Retry`` is deliberately throwaway: phase 0 ships no policies, but the core is only
meaningfully exercised by something that retries, delays and inspects outcomes. Phase 1
deletes it in favour of the real ``RetryPolicy``.
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine, Generator
from typing import Any

from failsafe._core import INVOKE, Acquire, Execution, Instruction, Outcome, Sleep, Step


class Retry:
    """A minimal retry policy: retries failures up to ``max_retries`` times."""

    def __init__(self, max_retries: int = 2, delay: float = 0.0) -> None:
        self.max_retries = max_retries
        self.delay = delay

    def apply(self, inner: Step) -> Step:
        def step(ex: Execution) -> Generator[Instruction, Any, Outcome[Any]]:
            failures = 0
            while True:
                outcome = yield from inner(ex)
                if outcome.exc is None:
                    return outcome
                failures += 1
                if failures > self.max_retries:
                    return outcome
                if self.delay:
                    yield Sleep(self.delay)

        return step


class Backoff:
    """A retry policy whose delay doubles per attempt, tracked per execution.

    The per-execution state lives in the generator's local scope, which is what keeps a
    shared policy object correct under concurrency (PLAN section 8 item 3).
    """

    def __init__(self, delay: float, max_retries: int = 3) -> None:
        self.delay = delay
        self.max_retries = max_retries

    def apply(self, inner: Step) -> Step:
        def step(ex: Execution) -> Generator[Instruction, Any, Outcome[Any]]:
            failures = 0
            last_delay = self.delay
            while True:
                outcome = yield from inner(ex)
                if outcome.exc is None:
                    return outcome
                failures += 1
                if failures > self.max_retries:
                    return outcome
                yield Sleep(last_delay)
                last_delay *= 2

        return step


class Bulkhead:
    """A permit policy, acquiring per attempt and releasing in a ``finally``."""

    def __init__(self, semaphore: Any, timeout: float | None = None) -> None:
        self.semaphore = semaphore
        self.timeout = timeout

    def apply(self, inner: Step) -> Step:
        def step(ex: Execution) -> Generator[Instruction, Any, Outcome[Any]]:
            acquired = yield Acquire(self.semaphore, self.timeout)
            if not acquired:
                return Outcome(exc=RuntimeError("bulkhead full"))
            try:
                return (yield from inner(ex))
            finally:
                self.semaphore.release()

        return step


class CountingBreaker:
    """A circuit breaker with just enough state to prove cross-mode sharing."""

    def __init__(self, failure_threshold: int = 1) -> None:
        self.failure_threshold = failure_threshold
        self.consecutive_failures = 0
        self.rejected = 0

    @property
    def is_open(self) -> bool:
        return self.consecutive_failures >= self.failure_threshold

    def apply(self, inner: Step) -> Step:
        def step(ex: Execution) -> Generator[Instruction, Any, Outcome[Any]]:
            if self.is_open:
                self.rejected += 1
                ex.attempts += 1
                return Outcome(exc=BreakerOpen())
            outcome = yield from inner(ex)
            if outcome.exc is None:
                self.consecutive_failures = 0
            else:
                self.consecutive_failures += 1
            return outcome

        return step


class BreakerOpen(Exception):
    """Raised by ``CountingBreaker`` when the circuit is open."""


class Tracer:
    """A policy that only forwards, for asserting composition order."""

    def __init__(self, name: str, log: list[str]) -> None:
        self.name = name
        self.log = log

    def apply(self, inner: Step) -> Step:
        def step(ex: Execution) -> Generator[Instruction, Any, Outcome[Any]]:
            self.log.append(f"enter {self.name}")
            outcome = yield from inner(ex)
            self.log.append(f"exit {self.name}")
            return outcome

        return step


def flaky(failures: int, result: str = "ok") -> Callable[[], str]:
    """Return a function that raises ``ConnectionError`` ``failures`` times, then succeeds."""
    calls = 0

    def fn() -> str:
        nonlocal calls
        calls += 1
        if calls <= failures:
            raise ConnectionError(f"boom {calls}")
        return result

    return fn


def aflaky(failures: int, result: str = "ok") -> Callable[[], Coroutine[Any, Any, str]]:
    """The coroutine counterpart of :func:`flaky`."""
    calls = 0

    async def fn() -> str:
        nonlocal calls
        calls += 1
        if calls <= failures:
            raise ConnectionError(f"boom {calls}")
        return result

    return fn


__all__ = [
    "INVOKE",
    "Backoff",
    "BreakerOpen",
    "Bulkhead",
    "CountingBreaker",
    "Retry",
    "Tracer",
    "aflaky",
    "flaky",
]
