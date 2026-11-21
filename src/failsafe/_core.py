"""The sans-io execution core: outcomes, instructions, ``Execution``, composition.

Policy logic is written once, as a generator that yields instructions. A driver
(:mod:`failsafe._sync` or :mod:`failsafe._async`) decides how to service them, which
is the only place sync and async differ. One policy instance therefore drives both,
sharing its state across them (D-7).

Nothing here imports :mod:`asyncio` or calls :func:`time.sleep`; blocking belongs to
the drivers.
"""

from __future__ import annotations

import contextvars
import time
from collections.abc import Callable, Generator
from dataclasses import dataclass
from typing import Any, Generic, Protocol, TypeVar

__all__ = [
    "Acquire",
    "Clock",
    "Execution",
    "FakeClock",
    "Instruction",
    "Invoke",
    "Outcome",
    "Policy",
    "Step",
    "compose",
    "current_execution",
    "set_current_execution",
]

R = TypeVar("R")


@dataclass(slots=True)
class Outcome(Generic[R]):
    """The result of an execution attempt: a value or an exception, never both.

    The analogue of failsafe-go's ``common.PolicyResult``. Policies pass outcomes
    between layers so that a failure can be inspected without unwinding the stack;
    only the driver boundary turns one back into a return or a raise (D-2).
    """

    value: R | None = None
    exc: Exception | None = None

    def unwrap(self) -> R:
        """Return the value, or raise the exception if this outcome holds one."""
        if self.exc is not None:
            raise self.exc
        return self.value  # type: ignore[return-value]


class Clock(Protocol):
    """The time seam. Every delay in the library goes through one of these.

    Tests install a :class:`FakeClock` so that no suite ever sleeps for real.
    """

    def now(self) -> float:
        """Return a monotonically increasing time in seconds."""

    def sleep(self, seconds: float) -> None:
        """Block the calling thread for ``seconds``."""

    async def sleep_async(self, seconds: float) -> None:
        """Suspend the calling task for ``seconds``."""


class SystemClock:
    """The default :class:`Clock`, backed by :func:`time.monotonic`."""

    __slots__ = ()

    def now(self) -> float:
        """Return :func:`time.monotonic`."""
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        """Block the calling thread via :func:`time.sleep`."""
        time.sleep(seconds)

    async def sleep_async(self, seconds: float) -> None:
        """Suspend the calling task via :func:`asyncio.sleep`."""
        import asyncio

        await asyncio.sleep(seconds)


SYSTEM_CLOCK = SystemClock()


class FakeClock:
    """A :class:`Clock` whose time only moves when something sleeps on it.

    Sleeping advances the clock instantly, so tests of delays, backoffs and deadlines
    are both exact and fast.

    Args:
        now: The initial time.
    """

    __slots__ = ("_now", "slept")

    def __init__(self, now: float = 0.0) -> None:
        self._now = now
        self.slept: list[float] = []
        """Every duration slept, in order. The record a delay test asserts against."""

    def now(self) -> float:
        """Return the current fake time."""
        return self._now

    def advance(self, seconds: float) -> None:
        """Move the clock forward without recording a sleep."""
        self._now += seconds

    def sleep(self, seconds: float) -> None:
        """Advance the clock by ``seconds`` and record it."""
        self.slept.append(seconds)
        self._now += seconds

    async def sleep_async(self, seconds: float) -> None:
        """Advance the clock by ``seconds`` and record it, without yielding to the loop."""
        self.sleep(seconds)


class Execution:
    """Information about an execution, shared by every policy layer and attempt.

    Mirrors failsafe-go's ``ExecutionInfo`` / ``ExecutionAttempt``. There is no
    ``cancelled`` flag: sync mode supports deadlines rather than cancellation, and
    async cancellation surfaces as :exc:`asyncio.CancelledError` (D-5).

    Attributes are maintained by the core; user code reads them via
    :func:`failsafe.current_execution`.
    """

    __slots__ = (
        "_clock",
        "attempt_start_time",
        "attempts",
        "deadline",
        "executions",
        "last_exception",
        "last_result",
        "retries",
        "start_time",
    )

    def __init__(self, clock: Clock = SYSTEM_CLOCK, deadline: float | None = None) -> None:
        now = clock.now()
        self._clock = clock
        self.start_time = now
        """The time the execution began, on the configured clock."""
        self.attempt_start_time = now
        """The time the most recent attempt began."""
        self.deadline = deadline
        """The absolute time by which the execution must finish, if any."""
        self.attempts = 0
        """Attempts started so far, including ones blocked before reaching the function."""
        self.executions = 0
        """Attempts that reached the function and completed."""
        self.retries = 0
        """Attempts after the first."""
        self.last_result: Any = None
        """The value from the most recent completed attempt."""
        self.last_exception: Exception | None = None
        """The exception from the most recent completed attempt."""

    @property
    def elapsed(self) -> float:
        """Seconds since the execution began."""
        return self._clock.now() - self.start_time

    @property
    def elapsed_attempt(self) -> float:
        """Seconds since the most recent attempt began."""
        return self._clock.now() - self.attempt_start_time

    @property
    def is_first_attempt(self) -> bool:
        """Whether this is the first attempt."""
        return self.attempts == 1

    @property
    def is_retry(self) -> bool:
        """Whether this attempt follows an earlier one."""
        return self.attempts > 1

    def remaining(self) -> float | None:
        """Seconds left before the deadline, clamped at zero, or ``None`` if unset.

        Sync code should pass this to whatever I/O timeout it is about to perform;
        the drivers only enforce deadlines between attempts.
        """
        if self.deadline is None:
            return None
        return max(0.0, self.deadline - self._clock.now())


_CURRENT: contextvars.ContextVar[Execution] = contextvars.ContextVar("failsafe_execution")


def current_execution() -> Execution | None:
    """Return the :class:`Execution` in progress, or ``None`` outside one.

    The replacement for failsafe-go's ``GetWithExecution``: a function running under a
    policy composition reads its attempt count, elapsed time or remaining deadline
    without every caller threading a parameter through.

    Example:
        >>> def connect(host):
        ...     ex = current_execution()
        ...     return do_io(host, timeout=ex.remaining() if ex else None)
    """
    return _CURRENT.get(None)


def set_current_execution(ex: Execution) -> contextvars.Token[Execution]:
    """Make ``ex`` the current execution, returning a token to restore the previous one."""
    return _CURRENT.set(ex)


@dataclass(frozen=True, slots=True)
class Invoke:
    """Call the user function and capture its outcome.

    The driver catches :class:`Exception`, never :class:`BaseException`, so a
    :exc:`KeyboardInterrupt` or :exc:`asyncio.CancelledError` propagates rather than
    becoming a retryable failure.
    """


INVOKE = Invoke()
"""The singleton :class:`Invoke`, yielded instead of allocating one per attempt."""


@dataclass(frozen=True, slots=True)
class Sleep:
    """Wait for ``seconds`` before resuming the policy.

    Serviced by :func:`time.sleep` or :func:`asyncio.sleep` via the clock, so an async
    sleep remains interruptible by cancellation.
    """

    seconds: float


@dataclass(frozen=True, slots=True)
class Acquire:
    """Acquire a permit from ``semaphore``, blocking up to ``timeout`` seconds.

    Sent back to the policy as a :class:`bool`. The policy is responsible for
    releasing the permit in a ``finally`` block, which also covers a driver
    abandoning the execution (PLAN section 8 item 7).
    """

    semaphore: Any
    timeout: float | None = None


Instruction = Invoke | Sleep | Acquire
"""Anything a policy may yield to a driver."""

Step = Callable[[Execution], Generator[Instruction, Any, Outcome[Any]]]
"""A composed policy layer: given an execution, a generator producing an outcome."""


class Policy(Protocol):
    """What the core requires of a policy: the ability to wrap an inner step.

    Structural, so a policy needs no base class. Implementations are built once per
    composition rather than per call.
    """

    def apply(self, inner: Step) -> Step:
        """Return a step that wraps ``inner`` with this policy's behaviour."""
        ...


def _attempt(ex: Execution) -> Generator[Instruction, Any, Outcome[Any]]:
    """The innermost step: one attempt, with its bookkeeping."""
    ex.attempts += 1
    if ex.attempts > 1:
        ex.retries += 1
        ex.attempt_start_time = ex._clock.now()
    outcome: Outcome[Any] = yield INVOKE
    ex.executions += 1
    ex.last_result = outcome.value
    ex.last_exception = outcome.exc
    return outcome


def compose(policies: tuple[Policy, ...]) -> Step:
    """Compose policies around the user function, innermost policy last.

    ``compose((fallback, retry, breaker))`` produces ``fallback(retry(breaker(fn)))``,
    matching failsafe-go's ``failsafe.With(...)`` ordering, where the outermost policy
    is called first and the innermost handles the function's result first.

    Called once per :class:`failsafe.Failsafe`, never per call: re-composing per call
    costs roughly as much as the rest of the core put together.

    Args:
        policies: Policies in outermost-to-innermost order.

    Returns:
        The outermost step.
    """
    step: Step = _attempt
    for policy in reversed(policies):
        step = policy.apply(step)
    return step
