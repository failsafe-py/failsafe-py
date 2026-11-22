"""Failure conditions: one vocabulary for every "does this result-or-error match?" filter.

Go needs four builder families (``HandleErrors``, ``HandleErrorTypes``, ``HandleResult``,
``HandleIf``) because ``errors.Is`` is not ``errors.As`` and generics split results from
errors. `isinstance` collapses all four into one type, reused wherever Go has
a condition family: ``handle=`` and ``abort=`` on retry, ``handle=`` on fallback and the
circuit breaker.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, TypeAlias

__all__ = ["Condition"]

_MAX_CAUSE_DEPTH = 20


Predicate: TypeAlias = Callable[[Any, "Exception | None"], bool]

UNSET: Any = object()
"""The default for every condition keyword, meaning "not configured".

A plain ``None`` default cannot serve: ``handle=None`` has to mean "a ``None`` return
is a failure", which is indistinguishable from omitting the argument otherwise.
"""

Condition: TypeAlias = Any
"""Anything that can decide whether an outcome is a failure.

One of:

- an exception type, matched against the exception and its ``__cause__`` chain
- a tuple or list of exception types or predicates, OR'ed together
- a callable taking ``(result, exception)`` and returning a bool
- any other value, matched against the returned value with ``==``

A value that would read as one of the above - a tuple, a list, anything callable,
including a class - needs a predicate: ``handle=lambda result, exc: result == (1, 2)``.

Typed as `Any` deliberately. Bare values are accepted, so the set of
legal arguments is "almost anything", and a union spelling that out would reject
valid code while promising a precision it cannot deliver. The constructor raises
`TypeError` on the shapes that are genuinely ambiguous.
"""


def matches_exception(exc: BaseException | None, types: tuple[type[Exception], ...]) -> bool:
    """Whether ``exc`` or anything it was explicitly raised from is one of ``types``.

    ``errors.Is`` follows Go's ``Unwrap``; `isinstance` follows nothing, so the
    chain is walked here. Only ``__cause__`` is followed, never ``__context__``: implicit
    context would match exceptions raised incidentally while handling another, which Go
    has no equivalent of. The walk is depth-limited and cycle-safe.

    An `ExceptionGroup` matches if any of its leaves does.
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


def _is_exception_type(item: Any) -> bool:
    """Whether ``item`` is an exception class rather than a value."""
    return isinstance(item, type) and issubclass(item, Exception)


def _value_predicate(expected: Any) -> tuple[Predicate, bool]:
    """Match a returned value, which never inspects the exception."""
    return lambda result, exc: exc is None and bool(result == expected), False


def _as_predicate(condition: Any) -> tuple[Predicate, bool]:
    """Compile one condition into a predicate, and whether it inspects errors."""
    if _is_exception_type(condition):
        types = (condition,)
        return lambda result, exc: matches_exception(exc, types), True
    if isinstance(condition, (str, bytes)):
        # Checked before Sequence, which a str satisfies, so it reads as one value.
        return _value_predicate(condition)
    if isinstance(condition, Sequence):
        return _as_sequence_predicate(condition)
    if callable(condition):
        predicate: Predicate = condition
        return predicate, True
    return _value_predicate(condition)


def _as_sequence_predicate(condition: Sequence[Any]) -> tuple[Predicate, bool]:
    """Compile a tuple or list of conditions, OR'ed together.

    A collection means "any of these", so its members must be conditions in their own
    right: exception types or predicates. A value belongs in a predicate.
    """
    if not condition:
        raise TypeError(
            "invalid condition (): an empty collection matches nothing, which is "
            "never useful; omit the argument instead"
        )
    bare = [item for item in condition if not (_is_exception_type(item) or callable(item))]
    if bare:
        offender = bare[0]
        raise TypeError(
            f"invalid condition {condition!r}: {offender!r} is ambiguous inside a "
            f"collection, which already means a set of conditions. Match it with a "
            f"predicate, as lambda result, exc: result == {offender!r}."
        )
    compiled = [_as_predicate(item) for item in condition]
    predicates = [p for p, _ in compiled]
    checks_errors = any(checks for _, checks in compiled)
    return (
        lambda result, exc: any(predicate(result, exc) for predicate in predicates),
        checks_errors,
    )


class FailureConditions:
    """A compiled set of conditions, evaluated as Go's ``BaseFailurePolicy`` does.

    Conditions are OR'ed. The subtle rule, preserved verbatim from Go's ``errorsChecked``
    flag: a result-only condition does **not** replace the default "any error is a
    failure" handling, because it never inspects errors at all. Configuring
    ``handle=None`` therefore leaves exceptions failing as before, while
    ``handle=ValueError`` narrows failures to that type.

    Compiled once at policy construction.

    Args:
        condition: The configured condition, or `UNSET` for Go's default.
    """

    __slots__ = ("_checks_errors", "_predicates")

    def __init__(self, condition: Condition = UNSET) -> None:
        if condition is UNSET:
            self._predicates: list[Predicate] = []
            self._checks_errors = False
            return
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
