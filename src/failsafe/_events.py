"""Events and listener dispatch.

Mirrors Go's ``events.go``. Two deviations, both deliberate:

- **Sync listeners only.** Awaiting a coroutine listener is impossible in sync mode, so
  allowing them would leak function colouring into the event model.
- **Listener exceptions are logged, not propagated.** Go lets a panic in a listener
  escape and abort the execution; a metrics bug must not break a production request.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
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
    """A registered set of listeners for one event, called in registration order.

    Accepts a single callable or a sequence of them. Exceptions raised by a
    listener are logged and swallowed: an execution's outcome never depends on whether
    a metrics hook worked.

    Args:
        listeners: The initially configured listener or listeners.
    """

    __slots__ = ("_listeners", "_name")

    def __init__(self, name: str, listeners: Listener | Sequence[Listener] | None) -> None:
        self._name = name
        if listeners is None:
            self._listeners: list[Listener] = []
        elif callable(listeners):
            self._listeners = [listeners]
        else:
            self._listeners = list(listeners)

    def add(self, listener: Listener) -> Listener:
        """Register ``listener`` and return it, so this doubles as a decorator.

        Example:
            >>> @retry.on_retry
            ... def log_retry(event):
            ...     log.warning("attempt %d failed", event.attempts)
        """
        self._listeners.append(listener)
        return listener

    def __bool__(self) -> bool:
        """Whether any listener is registered."""
        return bool(self._listeners)

    def emit(self, event: Any) -> None:
        """Call every listener with ``event``, logging and swallowing any exception."""
        for listener in self._listeners:
            try:
                listener(event)
            except Exception:
                _log.exception("failsafe %s listener raised", self._name)
