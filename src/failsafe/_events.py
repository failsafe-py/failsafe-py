"""Events and listener dispatch.

Mirrors Go's ``events.go``. Two deviations, both deliberate:

- **Sync listeners only.** Awaiting a coroutine listener is impossible in sync mode, so
  allowing them would leak function colouring into the event model.
- **Listener exceptions are logged, not propagated.** Go lets a panic in a listener
  escape and abort the execution; a metrics bug must not break a production request.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeAlias

if TYPE_CHECKING:
    from ._core import Execution

__all__ = [
    "ExecutionCompletedEvent",
    "ExecutionEvent",
    "ScheduledRetryEvent",
    "StateChangedEvent",
]

_log = logging.getLogger("failsafe")


@dataclass(frozen=True, slots=True)
class ExecutionEvent:
    """An execution attempt was evaluated by a policy.

    Attributes:
        execution: The execution, for its attempt counts and timings.
        result: The value from the attempt being reported, if any.
        exception: The exception from the attempt being reported, if any.
    """

    execution: Execution
    result: Any = None
    exception: Exception | None = None

    @property
    def attempts(self) -> int:
        """Attempts so far, including this one."""
        return self.execution.attempts

    @property
    def retries(self) -> int:
        """Retries so far."""
        return self.execution.retries

    @property
    def elapsed(self) -> float:
        """Seconds since the execution began."""
        return self.execution.elapsed


@dataclass(frozen=True, slots=True)
class ScheduledRetryEvent:
    """A retry has been scheduled and is about to be delayed.

    Fired before the delay elapses, unlike ``on_retry`` which fires after.

    Attributes:
        execution: The execution being retried.
        result: The value from the failed attempt, if any.
        exception: The exception from the failed attempt, if any.
        delay: Seconds the driver is about to wait, after jitter and clamping.
    """

    execution: Execution
    result: Any = None
    exception: Exception | None = None
    delay: float = 0.0


@dataclass(frozen=True, slots=True)
class ExecutionCompletedEvent:
    """An execution finished, successfully or not.

    Attributes:
        execution: The execution that finished.
        result: The final value, if any.
        exception: The final exception, if any.
    """

    execution: Execution
    result: Any = None
    exception: Exception | None = None


@dataclass(frozen=True, slots=True)
class StateChangedEvent:
    """A circuit breaker changed state.

    Attributes:
        previous_state: The state being left.
        state: The state being entered.
    """

    previous_state: str
    state: str


Listener: TypeAlias = Callable[[Any], None]


class Listeners:
    """One optional listener for an event, configured when the policy is built.

    Exceptions raised by a listener are logged and swallowed: an execution's outcome
    never depends on whether a metrics hook worked.

    Args:
        name: The event name, used in the log message when a listener raises.
        listener: The configured listener, if any.
    """

    __slots__ = ("_listener", "_name")

    def __init__(self, name: str, listener: Listener | None) -> None:
        self._name = name
        self._listener = listener

    def __bool__(self) -> bool:
        """Whether a listener is configured."""
        return self._listener is not None

    def emit(self, event: Any) -> None:
        """Call the listener with ``event``, logging and swallowing any exception."""
        if self._listener is None:
            return
        try:
            self._listener(event)
        except Exception:
            _log.exception("failsafe %s listener raised", self._name)
