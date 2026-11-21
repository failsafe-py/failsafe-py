"""Shared test fixtures.

``Tracer`` is a test double for composition ordering, which no shipped policy
exercises directly.
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine, Generator
from typing import Any

import pytest

from failsafe._core import Execution, FakeClock, Instruction, Outcome, Step


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
