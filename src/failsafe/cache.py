"""Read-through caching of execution results, ported from failsafe-go's ``cachepolicy``."""

from __future__ import annotations

from collections.abc import Callable, Generator
from typing import Any, Protocol

from ._conditions import UNSET, Condition, FailureConditions
from ._core import Execution, Instruction, Outcome, Step
from ._events import ExecutionCompletedEvent, ExecutionEvent, Listener, Listeners

__all__ = ["Cache", "CachePolicy"]


class Cache(Protocol):
    """The two operations a cache backend must provide.

    Structural, so a `dict` subclass, an LRU, or a Redis wrapper all satisfy it
    without importing anything from this library. Implementations are responsible for
    their own eviction, expiry and thread safety.
    """

    def get(self, key: str) -> tuple[Any, bool]:
        """Return the value for ``key`` and whether it was present.

        The flag distinguishes a missing entry from a cached ``None``.
        """
        ...

    def set(self, key: str, value: Any) -> None:
        """Store ``value`` under ``key``."""
        ...


class CachePolicy:
    """Serves results from a cache, skipping the execution entirely on a hit.

    The key is a function of the call arguments, shaped like
    `functools.lru_cache`::

        cache = CachePolicy(backend, key=lambda user_id: f"user:{user_id}")
        users = Failsafe(cache)
        users.call(fetch_user, 42)

    Composed outermost, a hit skips every policy inside it, which is usually what you
    want: a cached result needs no retry, no circuit breaker and no rate limiter.

    Only successful executions are cached by default, since caching a failure serves
    it to every later caller. ``cache_if`` narrows that further.

    Args:
        cache: The backend, satisfying [`Cache`][failsafe.Cache].
        key: Called with the execution's arguments, returning the cache key, or
            ``None`` to skip the cache for this call.
        cache_if: Which outcomes to cache. Defaults to any outcome that did not raise.
        on_hit: Called when a cached result is served.
        on_miss: Called when no cached result was found.
        on_cached: Called when a result is stored.

    Attributes:
        on_hit: Listeners called on a cache hit.
        on_miss: Listeners called on a cache miss.
        on_cached: Listeners called when a result is stored.
    """

    __slots__ = ("_cache", "_cache_if", "_key", "on_cached", "on_hit", "on_miss")

    def __init__(
        self,
        cache: Cache,
        *,
        key: Callable[..., str | None],
        cache_if: Condition = UNSET,
        on_hit: Listener | None = None,
        on_miss: Listener | None = None,
        on_cached: Listener | None = None,
    ) -> None:
        self._cache = cache
        self._key = key
        self._cache_if = FailureConditions(cache_if) if cache_if is not UNSET else None
        self.on_hit = Listeners("on_hit", on_hit)
        """Listeners called on a cache hit."""
        self.on_miss = Listeners("on_miss", on_miss)
        """Listeners called on a cache miss."""
        self.on_cached = Listeners("on_cached", on_cached)
        """Listeners called when a result is stored."""

    def apply(self, inner: Step) -> Step:
        """Wrap ``inner`` so a cached result short-circuits it."""

        def step(ex: Execution) -> Generator[Instruction, Any, Outcome[Any]]:
            key = self._key(*ex.args, **ex.kwargs)
            if key is not None:
                value, found = self._cache.get(key)
                if found:
                    if self.on_hit:
                        self.on_hit.emit(ExecutionCompletedEvent(ex, value, None))
                    return Outcome(value=value)
            if self.on_miss:
                self.on_miss.emit(ExecutionEvent(ex, None, None))

            outcome = yield from inner(ex)

            if key is not None and self._should_cache(outcome):
                self._cache.set(key, outcome.value)
                if self.on_cached:
                    self.on_cached.emit(ExecutionEvent(ex, outcome.value, outcome.exc))
            return outcome

        return step

    def _should_cache(self, outcome: Outcome[Any]) -> bool:
        """Whether this outcome should be stored."""
        if self._cache_if is None:
            return outcome.exc is None
        return self._cache_if(outcome.value, outcome.exc)
