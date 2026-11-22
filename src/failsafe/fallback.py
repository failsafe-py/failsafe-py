"""Substitutes an alternative result when an execution fails."""

from __future__ import annotations

from collections.abc import Callable, Generator
from typing import Any

from ._conditions import UNSET, Condition, FailureConditions
from ._core import Execution, Instruction, Outcome, Step
from ._events import ExecutionCompletedEvent, Listener, Listeners

__all__ = ["Fallback"]

_UNSET: Any = object()


class Fallback:
    """Returns an alternative result when the execution it wraps fails.

    Usually the outermost policy, so that it catches whatever the inner ones could not
    handle. Note that by default *any* exception fires the fallback, including
    [`RetriesExceeded`][failsafe.RetriesExceeded] and
        [`CircuitBreakerOpen`][failsafe.CircuitBreakerOpen] from inner
    policies; narrow it with ``handle`` if that is not wanted.

    Example:
        ```python
        Failsafe(Fallback(value=CACHED), retry).call(fetch, key)
        Failsafe(Fallback(fn=lambda ex: from_cache(ex.last_exception))).call(fetch)
        ```

    Args:
        value: The result to return instead. Pass this or ``fn``, not both.
        fn: Called with the [`Execution`][failsafe.Execution] to compute the result. Its
            ``last_result`` and ``last_exception`` describe the failure being handled.
            An exception it raises replaces the original.
        handle: What counts as a failure worth falling back from. Defaults to any
            exception.
        on_fallback: Called when the fallback produces a result.
        on_success: Called when the execution did not fail.
        on_failure: Called when the execution failed and the fallback fired.

    Raises:
        ValueError: If neither or both of ``value`` and ``fn`` are given.
    """

    __slots__ = ("_fn", "_handle", "_value", "on_failure", "on_fallback", "on_success")

    def __init__(
        self,
        *,
        value: Any = _UNSET,
        fn: Callable[[Execution], Any] | None = None,
        handle: Condition = UNSET,
        on_fallback: Listener | None = None,
        on_success: Listener | None = None,
        on_failure: Listener | None = None,
    ) -> None:
        if (value is _UNSET) == (fn is None):
            raise ValueError("Fallback takes exactly one of value= or fn=")
        self._value = value
        self._fn = fn
        self._handle = FailureConditions(handle)
        self.on_fallback = Listeners("on_fallback", on_fallback)
        """Listeners called when the fallback produces a result."""
        self.on_success = Listeners("on_success", on_success)
        """Listeners called when the execution did not fail."""
        self.on_failure = Listeners("on_failure", on_failure)
        """Listeners called when the execution failed."""

    def is_failure(self, outcome: Outcome[Any]) -> bool:
        """Whether ``outcome`` is a failure this policy would handle."""
        return self._handle(outcome.value, outcome.exc)

    def apply(self, inner: Step) -> Step:
        """Wrap ``inner`` so that a failure is replaced by the fallback result."""

        def step(ex: Execution) -> Generator[Instruction, Any, Outcome[Any]]:
            outcome = yield from inner(ex)

            if not self.is_failure(outcome):
                if self.on_success:
                    self.on_success.emit(ExecutionCompletedEvent(ex, outcome.value, outcome.exc))
                return outcome

            if self.on_failure:
                self.on_failure.emit(ExecutionCompletedEvent(ex, outcome.value, outcome.exc))

            if self._fn is None:
                result: Outcome[Any] = Outcome(value=self._value)
            else:
                try:
                    result = Outcome(value=self._fn(ex))
                except Exception as exc:  # the fallback's own failure replaces the original
                    result = Outcome(exc=exc)

            if self.on_fallback:
                self.on_fallback.emit(ExecutionCompletedEvent(ex, result.value, result.exc))
            return result

        return step
