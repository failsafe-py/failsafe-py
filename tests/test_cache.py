"""Cache semantics, ported from failsafe-go's cachepolicy corpus."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from failsafe import (
    AsyncFailsafe,
    CachePolicy,
    CircuitBreaker,
    ExecutionCompletedEvent,
    ExecutionEvent,
    Failsafe,
    RetryPolicy,
    cache_key,
    current_cache_key,
)
from failsafe._core import FakeClock


class DictCache:
    """The smallest thing satisfying the ``Cache`` protocol."""

    def __init__(self) -> None:
        self.store: dict[str, Any] = {}
        self.sets = 0

    def get(self, key: str) -> tuple[Any, bool]:
        if key in self.store:
            return self.store[key], True
        return None, False

    def set(self, key: str, value: Any) -> None:
        self.store[key] = value
        self.sets += 1


def test_a_hit_skips_the_execution() -> None:
    """Go TestCache_Get: the second call never reaches the function."""
    cache = DictCache()
    calls: list[int] = []

    def fetch(user_id: int) -> str:
        calls.append(user_id)
        return f"user{user_id}"

    failsafe = Failsafe(CachePolicy(cache, key=lambda uid: f"u:{uid}"), clock=FakeClock())

    assert failsafe.call(fetch, 42) == "user42"
    assert failsafe.call(fetch, 42) == "user42"
    assert calls == [42], "the cached call must not reach the function"


def test_the_key_is_a_function_of_the_arguments() -> None:
    """Different arguments are different entries, lru_cache style."""
    cache = DictCache()
    calls: list[int] = []

    def fetch(user_id: int) -> str:
        calls.append(user_id)
        return f"user{user_id}"

    failsafe = Failsafe(CachePolicy(cache, key=lambda uid: f"u:{uid}"), clock=FakeClock())

    assert failsafe.call(fetch, 1) == "user1"
    assert failsafe.call(fetch, 2) == "user2"
    assert failsafe.call(fetch, 1) == "user1"
    assert calls == [1, 2]


def test_the_key_sees_keyword_arguments() -> None:
    cache = DictCache()
    calls: list[int] = []

    def fetch(*, user_id: int) -> str:
        calls.append(user_id)
        return f"user{user_id}"

    failsafe = Failsafe(CachePolicy(cache, key=lambda user_id: f"u:{user_id}"), clock=FakeClock())

    assert failsafe.call(fetch, user_id=7) == "user7"
    assert failsafe.call(fetch, user_id=7) == "user7"
    assert calls == [7]


def test_a_key_of_none_skips_the_cache() -> None:
    """Go's empty cache key: no key, no caching, and the execution always runs."""
    cache = DictCache()
    calls: list[int] = []

    def fetch(user_id: int) -> str:
        calls.append(user_id)
        return "value"

    failsafe = Failsafe(CachePolicy(cache, key=lambda _uid: None), clock=FakeClock())

    assert failsafe.call(fetch, 1) == "value"
    assert failsafe.call(fetch, 1) == "value"
    assert calls == [1, 1]
    assert cache.sets == 0


def test_a_cached_none_is_a_hit() -> None:
    """The found flag is why ``get`` returns a pair: ``None`` is a real value."""
    cache = DictCache()
    calls: list[int] = []

    def fetch(key: str) -> None:
        calls.append(1)
        return None

    failsafe = Failsafe(CachePolicy(cache, key=lambda k: k), clock=FakeClock())

    assert failsafe.call(fetch, "k") is None
    assert failsafe.call(fetch, "k") is None
    assert calls == [1]


def test_a_failure_is_not_cached() -> None:
    """Go TestCache/should not cache on error: a cached failure outlives the outage."""
    cache = DictCache()

    def fails(key: str) -> str:
        raise ValueError("boom")

    failsafe = Failsafe(CachePolicy(cache, key=lambda k: k), clock=FakeClock())

    with pytest.raises(ValueError, match="boom"):
        failsafe.call(fails, "k")
    assert cache.store == {}


def test_cache_if_narrows_what_is_stored() -> None:
    """Go TestCache/with condition."""
    cache = DictCache()
    policy = CachePolicy(cache, key=lambda k: k, cache_if="bar")
    failsafe = Failsafe(policy, clock=FakeClock())

    assert failsafe.call(lambda k: "bar", "matching") == "bar"
    assert failsafe.call(lambda k: "baz", "other") == "baz"

    assert cache.store == {"matching": "bar"}


def test_listeners_report_hits_misses_and_stores() -> None:
    cache = DictCache()
    hits: list[ExecutionCompletedEvent] = []
    misses: list[ExecutionEvent] = []
    stored: list[ExecutionEvent] = []
    policy = CachePolicy(
        cache,
        key=lambda k: k,
        on_hit=hits.append,
        on_miss=misses.append,
        on_cached=stored.append,
    )
    failsafe = Failsafe(policy, clock=FakeClock())

    failsafe.call(lambda k: "value", "k")
    failsafe.call(lambda k: "value", "k")

    assert len(misses) == 1
    assert len(stored) == 1
    assert len(hits) == 1
    assert hits[0].result == "value"


def test_a_hit_skips_the_policies_inside_it() -> None:
    """Composed outermost, a cached result needs no retry and no breaker."""
    cache = DictCache()
    breaker = CircuitBreaker(failure_threshold=3, clock=FakeClock())
    calls: list[int] = []

    def flaky(key: str) -> str:
        calls.append(1)
        if len(calls) == 1:
            raise ValueError("first")
        return "ok"

    failsafe = Failsafe(
        CachePolicy(cache, key=lambda k: k),
        RetryPolicy(max_retries=2),
        breaker,
        clock=FakeClock(),
    )

    assert failsafe.call(flaky, "k") == "ok"
    before = len(calls)
    assert failsafe.call(flaky, "k") == "ok"
    assert len(calls) == before, "the hit must not reach the breaker or the function"


def test_caching_works_in_async_mode() -> None:
    """One policy instance, both drivers, one cache."""
    cache = DictCache()
    calls: list[int] = []

    async def fetch(user_id: int) -> str:
        calls.append(user_id)
        return f"user{user_id}"

    failsafe = AsyncFailsafe(CachePolicy(cache, key=lambda uid: f"u:{uid}"), clock=FakeClock())

    async def main() -> tuple[str, str]:
        return await failsafe.call(fetch, 1), await failsafe.call(fetch, 1)

    assert asyncio.run(main()) == ("user1", "user1")
    assert calls == [1]


def test_one_cache_shared_between_sync_and_async() -> None:
    cache = DictCache()
    policy = CachePolicy(cache, key=lambda k: k)
    calls: list[int] = []

    def sync_fetch(key: str) -> str:
        calls.append(1)
        return "value"

    async def async_fetch(key: str) -> str:
        calls.append(1)
        return "value"

    assert Failsafe(policy, clock=FakeClock()).call(sync_fetch, "k") == "value"
    assert asyncio.run(AsyncFailsafe(policy, clock=FakeClock()).call(async_fetch, "k")) == "value"
    assert calls == [1], "the async call must hit what the sync call stored"


def test_a_cache_key_block_overrides_the_key_callable() -> None:
    """Go reads the key from the context, which beats the one on the policy."""
    cache = DictCache()
    calls: list[int] = []

    def fetch(user_id: int) -> str:
        calls.append(user_id)
        return f"user{user_id}"

    failsafe = Failsafe(CachePolicy(cache, key=lambda uid: f"arg:{uid}"), clock=FakeClock())

    with cache_key("ctx:override"):
        assert failsafe.call(fetch, 1) == "user1"
    assert list(cache.store) == ["ctx:override"]

    # Outside the block the callable is in charge again.
    assert failsafe.call(fetch, 1) == "user1"
    assert sorted(cache.store) == ["arg:1", "ctx:override"]
    assert calls == [1, 1]


def test_a_cache_key_block_serves_hits() -> None:
    """The override is a real key, not just a label: it hits on the way back in."""
    cache = DictCache()
    calls: list[int] = []

    def fetch(user_id: int) -> str:
        calls.append(user_id)
        return f"user{user_id}"

    failsafe = Failsafe(CachePolicy(cache, key=lambda uid: f"arg:{uid}"), clock=FakeClock())

    with cache_key("tenant:config"):
        assert failsafe.call(fetch, 1) == "user1"
        # Different arguments, same scoped key: still a hit.
        assert failsafe.call(fetch, 999) == "user1"

    assert calls == [1], "the second call was served from the cache"


def test_a_policy_may_rely_on_the_block_alone() -> None:
    """With no key= the contextvar is the only source, and absence means uncached."""
    cache = DictCache()
    calls: list[int] = []

    def fetch() -> str:
        calls.append(1)
        return "value"

    failsafe = Failsafe(CachePolicy(cache), clock=FakeClock())

    with cache_key("only:ctx"):
        assert failsafe.call(fetch) == "value"
        assert failsafe.call(fetch) == "value"
    assert calls == [1], "the block supplied a key, so the second call hit"

    assert failsafe.call(fetch) == "value"
    assert failsafe.call(fetch) == "value"
    assert calls == [1, 1, 1], "with no key at all, nothing is cached"


def test_the_cache_key_scope_is_restored() -> None:
    assert current_cache_key() is None
    with cache_key("outer"):
        assert current_cache_key() == "outer"
        with cache_key("inner"):
            assert current_cache_key() == "inner"
        assert current_cache_key() == "outer"
    assert current_cache_key() is None
