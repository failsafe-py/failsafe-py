"""Failure conditions: one vocabulary for every "does this result-or-error match?" filter.

Go needs four builder families (``HandleErrors``, ``HandleErrorTypes``, ``HandleResult``,
``HandleIf``) because ``errors.Is`` is not ``errors.As`` and generics split results from
errors. :func:`isinstance` collapses all four into one type (D-3), reused wherever Go has
a condition family: ``handle=`` and ``abort=`` on retry, ``handle=`` on fallback and the
circuit breaker.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, TypeAlias

__all__ = ["Condition", "Result"]

_MAX_CAUSE_DEPTH = 20


@dataclass(frozen=True, slots=True)
class Result:
    """Matches an execution that returned ``value``, the analogue of Go's ``HandleResult``.

    The wrapper disambiguates "match this result" from "match this exception type", which
    is what would otherwise force a separate ``handle_result=`` parameter: bare
    ``handle=None`` is ambiguous between "no condition" and "treat ``None`` as a failure".

    Example:
        >>> RetryPolicy(handle=Result(None))   # a None return is a failure
    """

    value: Any


Predicate: TypeAlias = Callable[[Any, "Exception | None"], bool]

Condition: TypeAlias = (
    "type[Exception] | tuple[type[Exception], ...] | Result | Predicate | Sequence[Any]"
)
"""Anything that can decide whether an outcome is a failure.

One of:

- an exception type or tuple of them, matched with :func:`isinstance`
- a :class:`Result`, matched against the returned value with ``==``
- a callable taking ``(result, exception)`` and returning a bool
- a sequence of any of the above, OR'ed together, as in Go
"""


def matches_exception(exc: BaseException | None, types: tuple[type[Exception], ...]) -> bool:
    """Whether ``exc`` or anything it was explicitly raised from is one of ``types``.

    ``errors.Is`` follows Go's ``Unwrap``; :func:`isinstance` follows nothing, so the
    chain is walked here. Only ``__cause__`` is followed, never ``__context__``: implicit
    context would match exceptions raised incidentally while handling another, which Go
    has no equivalent of. The walk is depth-limited and cycle-safe.

    An :exc:`ExceptionGroup` matches if any of its leaves does.
    """
    seen: set[int] = set()
    current: BaseException | None = exc
    for _ in range(_MAX_CAUSE_DEPTH):
        if current is None or id(current) in seen:
            return False
        seen.add(id(current))
        if isinstance(current, types):
            return True
        if isinstance(current, BaseExceptionGroup) and any(
            matches_exception(leaf, types) for leaf in current.exceptions
        ):
            return True
        current = current.__cause__
    return False


def _as_predicate(condition: Any) -> tuple[Predicate, bool]:
    """Compile one condition into a predicate, and whether it inspects errors."""
    if isinstance(condition, Result):
        expected = condition.value
        return lambda result, exc: exc is None and bool(result == expected), False
    if isinstance(condition, type) and issubclass(condition, Exception):
        types = (condition,)
        return lambda result, exc: matches_exception(exc, types), True
    if isinstance(condition, tuple) and all(
        isinstance(item, type) and issubclass(item, Exception) for item in condition
    ):
        types = condition
        return lambda result, exc: matches_exception(exc, types), True
    if callable(condition):
        predicate: Predicate = condition
        return predicate, True
    if isinstance(condition, Sequence):
        compiled = [_as_predicate(item) for item in condition]
        predicates = [p for p, _ in compiled]
        checks_errors = any(checks for _, checks in compiled)
        return (
            lambda result, exc: any(predicate(result, exc) for predicate in predicates),
            checks_errors,
        )
    raise TypeError(
        f"invalid condition {condition!r}: expected an exception type, a tuple of them, "
        "Result(value), a callable, or a sequence of those"
    )


class FailureConditions:
    """A compiled set of conditions, evaluated as Go's ``BaseFailurePolicy`` does.

    Conditions are OR'ed. The subtle rule, preserved verbatim from Go's ``errorsChecked``
    flag: a result-only condition does **not** replace the default "any error is a
    failure" handling, because it never inspects errors at all. Configuring
    ``handle=Result(None)`` therefore leaves exceptions failing as before, while
    ``handle=ValueError`` narrows failures to that type.

    Compiled once at policy construction.

    Args:
        condition: The configured condition, or ``None`` for Go's default.
    """

    __slots__ = ("_checks_errors", "_predicates")

    def __init__(self, condition: Condition | None = None) -> None:
        if condition is None:
            self._predicates: list[Predicate] = []
            self._checks_errors = False
            return
        # A bare tuple is one condition (exception types); a list is a sequence of them.
        predicate, checks_errors = _as_predicate(condition)
        self._predicates = [predicate]
        self._checks_errors = checks_errors

    def __call__(self, result: Any, exc: Exception | None) -> bool:
        """Whether this outcome is a failure."""
        if not self._predicates:
            return exc is not None
        if any(predicate(result, exc) for predicate in self._predicates):
            return True
        return exc is not None and not self._checks_errors

    @property
    def configured(self) -> bool:
        """Whether any condition was configured."""
        return bool(self._predicates)
