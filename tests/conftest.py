"""Shared test fixtures.

``Bulkhead`` and ``Tracer`` are test doubles for engine behaviours that no shipped
policy exercises yet: permit acquisition and release-on-abandon (phase 3's real
``Bulkhead``), and composition ordering. They are deliberately minimal and are replaced
by the real policy when it lands.
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine, Generator
from typing import Any

import pytest

from failsafe._core import Acquire, Execution, FakeClock, Instruction, Outcome, Step


class Bulkhead:
    """Acquires a permit per attempt and releases it in a ``finally``.

    The ``finally`` is the point: a driver may abandon the generator at any instruction,
    and the permit must still come back (PLAN section 8 item 7).
    """

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


class Tracer:
    """Records entry and exit, for asserting composition order."""

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


@pytest.fixture
def clock() -> FakeClock:
    """A clock that advances only when something sleeps on it."""
    return FakeClock()


def flaky(failures: int, result: str = "ok") -> Callable[[], str]:
    """Return a function raising ``ConnectionError`` ``failures`` times, then succeeding."""
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
