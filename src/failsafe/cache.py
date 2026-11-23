"""Read-through caching of execution results."""

from __future__ import annotations

import contextlib
import contextvars
from collections.abc import Callable, Generator, Hashable, Iterator
from typing import Any, Protocol

from ._conditions import UNSET, Condition, FailureConditions
from ._core import Execution, Instruction, Outcome, Step
from ._events import ExecutionCompletedEvent, ExecutionEvent, Listener, Listeners

__all__ = ["Cache", "CachePolicy", "cache_key", "current_cache_key"]

_CACHE_KEY: contextvars.ContextVar[Hashable] = contextvars.ContextVar("failsafe_cache_key")


@contextlib.contextmanager
def cache_key(key: Hashable) -> Iterator[None]:
    """Set the cache key for the enclosed block, overriding a policy's ``key=``.

    Use this when the key belongs to request context rather than to the function's
    arguments:

    ```python
    with cache_key(f"{tenant}:config"):
        config = api.call(fetch_config)
    ```

    Args:
        key: The cache key for executions inside the block.
    """
    token = _CACHE_KEY.set(key)
    try:
        yield
    finally:
        _CACHE_KEY.reset(token)


def current_cache_key() -> Hashable | None:
    """Return the cache key in scope, or ``None``."""
    return _CACHE_KEY.get(None)


class Cache(Protocol):
    """A mutable cache backend.

    Any mapping, including `dict` and `cachetools` caches, satisfies this protocol.
    Backends are responsible for eviction, expiry and thread safety.
    """

    def __getitem__(self, key: Any) -> Any:
        """Return the cached value, or raise `KeyError` if absent."""
        ...

    def __setitem__(self, key: Any, value: Any) -> None:
        """Store ``value`` under ``key``."""
        ...


class CachePolicy:
    """Serves results from a cache, skipping the execution entirely on a hit.

    The key usually comes from the call arguments, shaped like
    `functools.lru_cache`:

    ```python
    cache = CachePolicy(backend, key=lambda user_id: f"user:{user_id}")
    users = Failsafe(cache)
    users.call(fetch_user, 42)
    ```

    A [`cache_key`][failsafe.cache_key] block overrides that, for keys belonging to
    the request rather than the arguments. One of the two must supply a key: with
    neither, the execution runs uncached.

    Composed outermost, a hit skips every policy inside it, which is usually what you
    want: a cached result needs no retry, no circuit breaker and no rate limiter.

    Only successful executions are cached by default, since caching a failure serves
    it to every later caller. ``cache_if`` narrows that further.

    Args:
        cache: The backend, satisfying [`Cache`][failsafe.Cache].
        key: Called with the execution's arguments, returning the cache key, or
            ``None`` to skip the cache for this call. A
            [`cache_key`][failsafe.cache_key] block takes precedence over it.
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
        key: Callable[..., Hashable | None] | None = None,
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
            key = self._resolve_key(ex)
            if key is not None:
                try:
                    value = self._cache[key]
                except KeyError:
                    pass
                else:
                    if self.on_hit:
                        self.on_hit.emit(ExecutionCompletedEvent(ex, value, None))
                    return Outcome(value=value)
            if self.on_miss:
                self.on_miss.emit(ExecutionEvent(ex, None, None))

            outcome = yield from inner(ex)

            if key is not None and self._should_cache(outcome):
                self._cache[key] = outcome.value
                if self.on_cached:
                    self.on_cached.emit(ExecutionEvent(ex, outcome.value, outcome.exc))
            return outcome

        return step

    def _resolve_key(self, ex: Execution) -> Hashable | None:
        """The cache key for this execution, or ``None`` to skip the cache.

        A surrounding [`cache_key`][failsafe.cache_key] block wins over ``key=``,
        matching Go, where a key in the context overrides the one on the policy.
        """
        scoped = current_cache_key()
        if scoped is not None:
            return scoped
        if self._key is None:
            return None
        return self._key(*ex.args, **ex.kwargs)

    def _should_cache(self, outcome: Outcome[Any]) -> bool:
        """Whether this outcome should be stored."""
        if self._cache_if is None:
            return outcome.exc is None
        return self._cache_if(outcome.value, outcome.exc)
