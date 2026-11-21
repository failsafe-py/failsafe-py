"""Delay strategies: the ``delay=`` vocabulary shared by retry and the circuit breaker.

Go spreads these across seven builder methods (``WithDelay``, ``WithDelayFunc``,
``WithBackoff``, ``WithBackoffFactor``, ``WithRandomDelay``, ``WithJitter``,
``WithJitterFactor``). One parameter taking one of a few small types replaces all of
them (D-1, D-6).

Jitter is an attribute of a delay rather than a top-level setting, because Go's own
docs note that jitter is ignored when no delay is configured: a top-level ``jitter=``
is a parameter that silently does nothing in a valid-looking config.
"""

from __future__ import annotations

import random
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any, TypeAlias

if TYPE_CHECKING:
    from ._core import Execution

__all__ = ["Backoff", "Delay", "Fixed", "Jitter", "RandomDelay", "seconds"]


def seconds(duration: float | timedelta) -> float:
    """Normalize a duration to float seconds.

    Durations are floats throughout, matching :func:`time.sleep`, :func:`asyncio.timeout`
    and httpx, but a :class:`~datetime.timedelta` is accepted anywhere one is given.

    Args:
        duration: Seconds, or a timedelta.

    Returns:
        The duration in seconds.
    """
    if isinstance(duration, timedelta):
        return duration.total_seconds()
    return float(duration)


@dataclass(frozen=True, slots=True)
class Jitter:
    """Randomness applied to a computed delay, to decorrelate retrying clients.

    Exactly one of ``factor`` or ``seconds`` applies, keeping Go's two distinct meanings
    without a polymorphic float where "below 1 means a factor" would silently misread
    ``jitter=0.5``.

    Args:
        factor: Vary each delay by ±``factor`` of itself, so ``0.25`` means ±25%.
        seconds: Vary each delay by ±``seconds``, an absolute amount.

    Raises:
        ValueError: If neither or both are given, or either is negative.
    """

    factor: float = 0.0
    seconds: float = 0.0

    def __post_init__(self) -> None:
        if self.factor < 0 or self.seconds < 0:
            raise ValueError("jitter must not be negative")
        if bool(self.factor) == bool(self.seconds):
            raise ValueError("Jitter takes exactly one of factor= or seconds=")

    def apply(self, delay: float, rng: random.Random) -> float:
        """Return ``delay`` perturbed by this jitter, never below zero."""
        if self.seconds:
            return max(0.0, delay + (1 - 2 * rng.random()) * self.seconds)
        return max(0.0, delay * (1 + (1 - 2 * rng.random()) * self.factor))


@dataclass(frozen=True, slots=True)
class Fixed:
    """The same delay before every attempt.

    The explicit form of a plain ``delay=1.0``, used when jitter is wanted.

    Args:
        delay: Seconds to wait, or a timedelta.
        jitter: Randomness to apply to each delay.
    """

    delay: float | timedelta
    jitter: Jitter | float | None = None

    def __iter__(self) -> Any:
        """Yield this delay, unchanged, forever."""
        value = seconds(self.delay)
        while True:
            yield value


@dataclass(frozen=True, slots=True)
class Backoff:
    """An exponentially increasing delay, capped at ``max_delay``.

    The delays are ``delay``, ``delay * factor``, ``delay * factor ** 2`` and so on,
    each capped at ``max_delay``.

    Args:
        delay: The first delay, in seconds or as a timedelta.
        max_delay: The cap on subsequent delays.
        factor: What each delay is multiplied by, 2.0 to double.
        jitter: Randomness to apply to each delay.

    Raises:
        ValueError: If ``factor`` is below 1.
    """

    delay: float | timedelta
    max_delay: float | timedelta
    factor: float = 2.0
    jitter: Jitter | float | None = None

    def __post_init__(self) -> None:
        if self.factor < 1:
            raise ValueError("backoff factor must be at least 1")

    def __iter__(self) -> Any:
        """Yield each successive backoff delay."""
        value = seconds(self.delay)
        cap = seconds(self.max_delay)
        while True:
            yield value
            value = min(value * self.factor, cap)


@dataclass(frozen=True, slots=True)
class RandomDelay:
    """A delay drawn uniformly from ``[min_delay, max_delay]`` before every attempt.

    Args:
        min_delay: The lower bound, in seconds or as a timedelta.
        max_delay: The upper bound.
        jitter: Randomness to apply on top of the draw. Rarely useful; the draw is
            already random.

    Raises:
        ValueError: If ``min_delay`` exceeds ``max_delay``.
    """

    min_delay: float | timedelta
    max_delay: float | timedelta
    jitter: Jitter | float | None = None

    def __post_init__(self) -> None:
        if seconds(self.min_delay) > seconds(self.max_delay):
            raise ValueError("min_delay must not exceed max_delay")

    def draws(self, rng: random.Random) -> Iterator[float]:
        """Yield each random draw, from the policy's injected RNG."""
        low = seconds(self.min_delay)
        high = seconds(self.max_delay)
        while True:
            yield low + rng.random() * (high - low)


Delay: TypeAlias = (
    "float | timedelta | Fixed | Backoff | RandomDelay | Callable[[Execution | None], float | None]"
)
"""How long to wait before the next attempt.

One of:

- a number of seconds or a :class:`~datetime.timedelta`, for a fixed delay
- :class:`Fixed`, :class:`Backoff` or :class:`RandomDelay`
- a callable returning seconds, or ``None`` for no delay. It receives the
  :class:`~failsafe.Execution`, or ``None`` when there is none to report: a circuit
  breaker opened by :meth:`~failsafe.CircuitBreaker.open` rather than by a failure has
  no execution to compute a delay from.
"""


def compile_delay(
    delay: Delay | None, jitter_rng: random.Random
) -> Callable[[Execution | None], float]:
    """Compile a delay into a function of the execution, called once per retry.

    The returned function closes over whatever per-execution state the strategy needs
    (a backoff's current value), so callers must build a fresh one per execution rather
    than per policy: shared policies must not share backoff state (PLAN section 8
    item 3).

    Args:
        delay: The configured delay, or ``None`` for no delay.
        jitter_rng: The source of jitter randomness.

    Returns:
        A function from the execution to a delay in seconds.
    """
    if delay is None:
        return lambda ex: 0.0
    if isinstance(delay, timedelta | int | float):
        fixed = seconds(delay)
        return lambda ex: fixed
    if isinstance(delay, Fixed | Backoff | RandomDelay):
        jitter = _as_jitter(delay.jitter)
        values = delay.draws(jitter_rng) if isinstance(delay, RandomDelay) else iter(delay)
        if jitter is None:
            return lambda ex: next(values)
        return lambda ex: jitter.apply(next(values), jitter_rng)
    if callable(delay):
        computed = delay
        # Go reaches a callable delay only through a recorded execution; a manual
        # transition passes nil and falls through to the *fixed* delay. Our delay= is
        # one union rather than Go's two fields, so a callable has no fixed delay to
        # fall back on and yields zero, as Go does when neither is configured.
        return lambda ex: 0.0 if ex is None else seconds(computed(ex) or 0.0)
    raise TypeError(
        f"invalid delay {delay!r}: expected seconds, a timedelta, Fixed, Backoff, "
        "RandomDelay, or a callable"
    )


def _as_jitter(jitter: Jitter | float | None) -> Jitter | None:
    """Accept a bare float as shorthand for ``Jitter(factor=...)``."""
    if jitter is None:
        return None
    if isinstance(jitter, Jitter):
        return jitter
    return Jitter(factor=jitter)
