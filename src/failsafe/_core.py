"""The sans-io execution core: outcomes, instructions, ``Execution``, composition.

Policy logic is written once, as a generator that yields instructions. A driver
(`_sync` or `_async`) decides how to service them, which
is the only place sync and async differ. One policy instance therefore drives both,
sharing its state across them.

Nothing here imports `asyncio` or calls `time.sleep`; blocking belongs to
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
    "Hedge",
    "HedgeControl",
    "Instruction",
    "Invoke",
    "Outcome",
    "Policy",
    "Race",
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
    only the driver boundary turns one back into a return or a raise.
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

    Tests install a `FakeClock` so that no suite ever sleeps for real.
    """

    def now(self) -> float:
        """Return a monotonically increasing time in seconds."""

    def sleep(self, seconds: float) -> None:
        """Block the calling thread for ``seconds``."""

    async def sleep_async(self, seconds: float) -> None:
        """Suspend the calling task for ``seconds``."""


class SystemClock:
    """The default `Clock`, backed by `time.monotonic`."""

    __slots__ = ()

    def now(self) -> float:
        """Return `time.monotonic`."""
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        """Block the calling thread via `time.sleep`."""
        time.sleep(seconds)

    async def sleep_async(self, seconds: float) -> None:
        """Suspend the calling task via `asyncio.sleep`."""
        import asyncio

        await asyncio.sleep(seconds)


SYSTEM_CLOCK = SystemClock()


class FakeClock:
    """A `Clock` whose time only moves when something sleeps on it.

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


@dataclass(slots=True)
class _Counters:
    """Counts shared by every attempt of one execution, including hedges."""

    attempts: int = 0
    executions: int = 0
    retries: int = 0
    hedges: int = 0


class Execution:
    """Information about an execution, shared by every policy layer and attempt.

    Mirrors failsafe-go's ``ExecutionInfo`` / ``ExecutionAttempt``. There is no
    ``cancelled`` flag: sync mode supports deadlines rather than cancellation, and
    async cancellation surfaces as `asyncio.CancelledError`.

    Attributes are maintained by the core; user code reads them via
    [`current_execution`][failsafe.current_execution].

    Hedged attempts run concurrently against copies of this object: the counts are
    shared, while the last result and the attempt's start time belong to the attempt.
    """

    __slots__ = (
        "_clock",
        "_counters",
        "args",
        "attempt_start_time",
        "deadline",
        "is_hedge",
        "kwargs",
        "last_exception",
        "last_result",
        "start_time",
    )

    def __init__(self, clock: Clock = SYSTEM_CLOCK, deadline: float | None = None) -> None:
        now = clock.now()
        self._clock = clock
        self._counters = _Counters()
        self.start_time = now
        """The time the execution began, on the configured clock."""
        self.attempt_start_time = now
        """The time the most recent attempt began."""
        self.deadline = deadline
        """The absolute time by which the execution must finish, if any."""
        self.is_hedge = False
        """Whether this attempt is a hedge rather than the initial attempt."""
        self.args: tuple[Any, ...] = ()
        """The positional arguments the execution was called with."""
        self.kwargs: dict[str, Any] = {}
        """The keyword arguments the execution was called with."""
        self.last_result: Any = None
        """The value from the most recent completed attempt."""
        self.last_exception: Exception | None = None
        """The exception from the most recent completed attempt."""

    @property
    def attempts(self) -> int:
        """Attempts started so far, including ones blocked before reaching the function."""
        return self._counters.attempts

    @attempts.setter
    def attempts(self, value: int) -> None:
        self._counters.attempts = value

    @property
    def executions(self) -> int:
        """Attempts that reached the function and completed."""
        return self._counters.executions

    @executions.setter
    def executions(self, value: int) -> None:
        self._counters.executions = value

    @property
    def retries(self) -> int:
        """Attempts after the first that were started by a retry, not a hedge."""
        return self._counters.retries

    @retries.setter
    def retries(self, value: int) -> None:
        self._counters.retries = value

    @property
    def hedges(self) -> int:
        """Hedged attempts started so far, including ones still running."""
        return self._counters.hedges

    def copy_for_hedge(self) -> Execution:
        """Return a copy for a hedged attempt, counting it as an attempt and a hedge.

        The copy shares this execution's counts and start time, so every attempt sees
        the same totals, but carries its own last result and attempt start time.
        """
        copy = object.__new__(Execution)
        copy._clock = self._clock
        copy._counters = self._counters
        copy.start_time = self.start_time
        copy.attempt_start_time = self._clock.now()
        copy.deadline = self.deadline
        copy.is_hedge = True
        copy.args = self.args
        copy.kwargs = self.kwargs
        copy.last_result = None
        copy.last_exception = None
        self._counters.attempts += 1
        self._counters.hedges += 1
        return copy

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
    """Return the [`Execution`][failsafe.Execution] in progress, or ``None`` outside one.

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

    The driver catches `Exception`, never `BaseException`, so a
    `KeyboardInterrupt` or `asyncio.CancelledError` propagates rather than
    becoming a retryable failure.
    """


INVOKE = Invoke()
"""The singleton `Invoke`, yielded instead of allocating one per attempt."""


@dataclass(frozen=True, slots=True)
class Sleep:
    """Wait for ``seconds`` before resuming the policy.

    Serviced by `time.sleep` or `asyncio.sleep` via the clock, so an async
    sleep remains interruptible by cancellation.
    """

    seconds: float


@dataclass(frozen=True, slots=True)
class Acquire:
    """Acquire a permit from ``semaphore``, blocking up to ``timeout`` seconds.

    Sent back to the policy as a `bool`. The policy is responsible for
    releasing the permit in a ``finally`` block, which also covers a driver
    abandoning the execution.
    """

    semaphore: Any
    timeout: float | None = None


@dataclass(frozen=True, slots=True)
class Race:
    """Run ``inner`` under a deadline, abandoning it after ``seconds``.

    Sent back to the policy as the inner `Outcome`, or ``None`` if the deadline
    elapsed first. The two drivers differ fundamentally here, which is why this is an
    instruction rather than policy code:

    - async interrupts the attempt mid-await via `asyncio.timeout`;
    - sync is cooperative, checking the deadline between instructions, unless
      ``interrupt`` asks for a worker thread.
    """

    inner: Any
    seconds: float
    interrupt: bool = False


class HedgeControl(Protocol):
    """What the drivers need from a hedge policy to run concurrent attempts.

    Keeps the policy's configuration — delays, cancellation conditions, budget — out
    of the core while leaving the concurrency to the driver.
    """

    def hedge_delay(self, ex: Execution) -> float | None:
        """Seconds to wait before starting another attempt, or ``None`` to stop."""
        ...

    def accepts(self, outcome: Outcome[Any]) -> bool:
        """Whether ``outcome`` should win and cancel the attempts still running."""
        ...

    def start_hedge(self, ex: Execution) -> bool:
        """Admit a hedged attempt, returning ``False`` to stop hedging."""
        ...

    def finish_hedge(self) -> None:
        """Release whatever `start_hedge` admitted."""
        ...

    def record_duration(self, ex: Execution) -> None:
        """Record how long one accepted attempt took."""
        ...


@dataclass(frozen=True, slots=True)
class Hedge:
    """Run ``inner`` concurrently against itself, returning the first accepted outcome.

    Attempts start one at a time: each is given a delay to finish before the next
    joins it. The first outcome ``control`` accepts wins and the rest are cancelled
    for real, which only asyncio can do, so the sync driver rejects this instruction.

    Sent back to the policy as the winning `Outcome`.
    """

    inner: Any
    control: HedgeControl


Instruction = Invoke | Sleep | Acquire | Race | Hedge
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
    # A hedge copy already counted itself and set its own start time.
    if not ex.is_hedge:
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

    Called once per [`Failsafe`][failsafe.Failsafe], never per call: re-composing per call
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
