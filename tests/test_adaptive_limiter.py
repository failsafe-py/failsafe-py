from __future__ import annotations

import asyncio
import random
import threading
from typing import Any

import pytest

from failsafe import AdaptiveLimiter, LimitChangedEvent, LimitExceeded
from failsafe._core import FakeClock
from failsafe.adaptive_limiter import _QueueStats

_WARMUP = 10


def make_limiter(**kwargs: Any) -> tuple[AdaptiveLimiter, FakeClock]:
    clock = FakeClock()
    limiter = AdaptiveLimiter(
        recent_window_min_duration=1.0,
        recent_window_max_duration=1.0,
        recent_window_min_samples=1,
        clock=clock,
        **kwargs,
    )
    for _ in range(_WARMUP):
        limiter._baseline_rtt.add(1.0)
    return limiter, clock


def record(limiter: AdaptiveLimiter, clock: FakeClock, rtt: float, inflight: int) -> None:
    assert limiter._semaphore.acquire(timeout=0)
    clock.advance(2.0)
    limiter._record(clock.now(), rtt, inflight, dropped=False)


def test_defaults() -> None:
    limiter = AdaptiveLimiter()

    assert limiter.limit == 20
    assert limiter.inflight == 0
    assert limiter.queued == 0
    assert limiter.max_inflight == 0


def test_initial_limit_is_configurable() -> None:
    limiter = AdaptiveLimiter(min_limit=1, max_limit=10, initial_limit=5)

    assert limiter.limit == 5


def test_acquire_permit_and_record_releases_the_permit() -> None:
    limiter = AdaptiveLimiter(min_limit=1, max_limit=20, initial_limit=1)

    permit = limiter.acquire_permit()
    assert limiter.inflight == 1
    permit.record()

    assert limiter.inflight == 0


def test_try_acquire_permit_returns_none_when_full() -> None:
    limiter = AdaptiveLimiter(min_limit=1, max_limit=20, initial_limit=1)

    permit = limiter.try_acquire_permit()
    assert permit is not None
    assert limiter.try_acquire_permit() is None

    permit.record()
    assert limiter.inflight == 0


def test_can_acquire_permit_tracks_capacity() -> None:
    limiter = AdaptiveLimiter(min_limit=1, max_limit=20, initial_limit=1)

    assert limiter.can_acquire_permit()
    permit = limiter.acquire_permit()
    assert not limiter.can_acquire_permit()

    permit.record()
    assert limiter.can_acquire_permit()


def test_acquire_permit_raises_when_full_without_queueing() -> None:
    limiter = AdaptiveLimiter(min_limit=1, max_limit=20, initial_limit=1)
    limiter.acquire_permit()

    with pytest.raises(LimitExceeded) as excinfo:
        limiter.acquire_permit()

    assert excinfo.value.limit == 1


def test_queued_executions_are_counted() -> None:
    limiter = AdaptiveLimiter(min_limit=1, max_limit=20, initial_limit=1, queueing=(2.0, 4.0))
    permit = limiter.acquire_permit()
    started = threading.Event()

    def acquire() -> None:
        started.set()
        limiter.acquire_permit().record()

    thread = threading.Thread(target=acquire, daemon=True)
    thread.start()
    started.wait(1.0)
    while limiter.queued == 0:  # the waiter has to reach the semaphore
        pass

    assert limiter.queued == 1
    permit.record()
    thread.join(1.0)
    assert limiter.queued == 0


def test_record_smooths_and_filters_rtts() -> None:
    """The median filter drops the 300ms outlier; the average smooths the rest."""
    limiter, clock = make_limiter()

    for rtt in (0.1, 0.3, 0.2, 0.2, 0.25):
        record(limiter, clock, rtt, inflight=10)

    assert limiter._median_filter.median() == pytest.approx(0.2)
    assert limiter._smoothed_recent_rtt.value == pytest.approx(0.2)


def test_record_increases_the_limit_when_times_are_flat() -> None:
    limiter, clock = make_limiter()

    record(limiter, clock, rtt=1.0, inflight=5)  # queue size 0
    assert limiter.limit == 21

    record(limiter, clock, rtt=0.5, inflight=5)  # queue size below alpha
    assert limiter.limit == 22


def test_record_decreases_the_limit_when_times_climb() -> None:
    limiter, clock = make_limiter()

    record(limiter, clock, rtt=2.0, inflight=10)  # queue size 10, above beta
    assert limiter.limit == 19

    record(limiter, clock, rtt=2.0, inflight=10)
    assert limiter.limit == 18


def test_record_holds_the_limit_between_alpha_and_beta() -> None:
    limiter, clock = make_limiter()

    record(limiter, clock, rtt=1.3, inflight=10)
    assert limiter.limit == 20

    record(limiter, clock, rtt=1.3, inflight=10)
    assert limiter.limit == 20


def test_limit_is_clamped_to_min_and_max() -> None:
    limiter, clock = make_limiter(min_limit=19, max_limit=21, initial_limit=20)

    for _ in range(5):
        record(limiter, clock, rtt=4.0, inflight=10)
    assert limiter.limit == 19

    for _ in range(10):
        record(limiter, clock, rtt=0.1, inflight=10)
    assert limiter.limit == 21


def test_stabilization_window_prevents_a_decrease_on_an_inflight_dip() -> None:
    """Without stabilization a brief traffic dip drags the max limit down with it."""
    limiter, clock = make_limiter(max_limit_factor=1.5, max_limit_stabilization_window=300.0)

    record(limiter, clock, rtt=1.0, inflight=20)
    assert limiter.limit == 21
    assert limiter._max_inflight_window.value == 20

    record(limiter, clock, rtt=1.0, inflight=5)
    assert limiter.limit == 22
    assert limiter._max_inflight_window.value == 20


def test_without_stabilization_an_inflight_dip_lowers_the_limit() -> None:
    limiter, clock = make_limiter(max_limit_factor=1.5)

    record(limiter, clock, rtt=1.0, inflight=20)
    assert limiter.limit == 21

    record(limiter, clock, rtt=1.0, inflight=5)
    assert limiter.limit == 20


def test_limit_decreases_once_the_stabilization_window_expires() -> None:
    limiter, clock = make_limiter(max_limit_factor=1.5, max_limit_stabilization_window=10.0)

    record(limiter, clock, rtt=1.0, inflight=20)
    assert limiter.limit == 21
    assert limiter._max_inflight_window.value == 20

    clock.advance(10.0)
    record(limiter, clock, rtt=1.0, inflight=5)
    assert limiter.limit == 20
    assert limiter._max_inflight_window.value == 5


def test_max_limit_factor_decay_tightens_headroom_as_load_grows() -> None:
    limiter = AdaptiveLimiter(
        max_limit_factor=5.0, max_limit_factor_decay=1.0, min_limit_factor=2.0
    )

    assert limiter._compute_max_limit(1) == pytest.approx(5.0)  # log10(1) == 0
    assert limiter._compute_max_limit(10) == pytest.approx(40.0)  # factor 4
    assert limiter._compute_max_limit(100) == pytest.approx(300.0)  # factor 3
    assert limiter._compute_max_limit(10_000) == pytest.approx(20_000.0)  # floored at 2


def test_on_limit_changed_fires_with_old_and_new_limits() -> None:
    events: list[LimitChangedEvent] = []
    limiter, clock = make_limiter(on_limit_changed=events.append)

    record(limiter, clock, rtt=1.0, inflight=5)

    assert events == [LimitChangedEvent(20, 21)]


def test_on_limit_changed_is_silent_when_the_limit_holds() -> None:
    events: list[LimitChangedEvent] = []
    limiter, clock = make_limiter(on_limit_changed=events.append)

    record(limiter, clock, rtt=1.3, inflight=10)

    assert events == []


def test_the_semaphore_follows_the_limit() -> None:
    """The limit is only real if the semaphore enforces it."""
    limiter, clock = make_limiter()

    record(limiter, clock, rtt=2.0, inflight=10)

    assert limiter.limit == 19
    assert limiter._semaphore.size == 19


def test_reset_restores_the_initial_state() -> None:
    limiter, clock = make_limiter(min_limit=5, max_limit=100, initial_limit=10)
    record(limiter, clock, rtt=4.0, inflight=10)
    assert limiter.limit == 9

    limiter.reset()

    assert limiter.limit == 10
    assert limiter._recent_rtt.size == 0
    assert limiter._baseline_rtt.value == 0.0
    assert limiter._smoothed_recent_rtt.value == 0.0
    assert limiter._median_filter.median() == 0.0
    assert limiter._semaphore.size == 10


def test_dropped_samples_do_not_move_the_limit() -> None:
    """A dropped execution's duration says nothing about the backend."""
    limiter, clock = make_limiter()

    assert limiter._semaphore.acquire(timeout=0)
    clock.advance(2.0)
    limiter._record(clock.now(), 0.0, 10, dropped=True)

    assert limiter.limit == 20
    assert limiter._recent_rtt.size == 0


def test_permit_drop_releases_without_recording() -> None:
    limiter, _ = make_limiter()

    permit = limiter.acquire_permit()
    permit.drop()

    assert limiter.inflight == 0
    assert limiter._recent_rtt.size == 0


def test_the_limit_does_not_update_before_the_window_elapses() -> None:
    limiter, clock = make_limiter()

    assert limiter._semaphore.acquire(timeout=0)
    limiter._record(clock.now(), 1.0, 5, dropped=False)

    assert limiter.limit == 20


@pytest.mark.parametrize(
    "kwargs",
    [
        {"min_limit": 2, "max_limit": 2, "initial_limit": 1},
        {"min_limit": 1, "max_limit": 2, "initial_limit": 3},
        {"min_limit": 0, "max_limit": 2, "initial_limit": 1},
        {"max_limit_factor": 0.5},
        {"max_limit_factor_decay": -1.0},
        {"min_limit_factor": 0.5},
        {"min_limit_factor": 6.0},
        {"recent_quantile": -1.0},
        {"recent_window_min_duration": 0.0},
        {"recent_window_max_duration": 0.0},
        {"recent_window_min_duration": 60.0, "recent_window_max_duration": 1.0},
        {"recent_window_min_samples": 0},
        {"baseline_window_age": 0},
        {"correlation_window_size": 0},
        {"max_limit_stabilization_window": -1.0},
        {"queueing": (0.5, 2.0)},
        {"queueing": (2.0, 1.0)},
    ],
)
def test_invalid_configuration_is_rejected(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        AdaptiveLimiter(**kwargs)  # type: ignore[arg-type]


def test_queueing_rejects_more_as_the_queue_builds() -> None:
    """No rejections below the initial factor, all of them at the max factor."""
    limiter = AdaptiveLimiter(
        min_limit=10,
        max_limit=10,
        initial_limit=10,
        queueing=(2.0, 4.0),
        rng=random.Random(42),
    )

    stats = limiter._queue_stats()
    assert stats.rejection_threshold == 20
    assert stats.max_queue == 40
    assert stats.compute_rejection_rate() == 0.0

    assert _QueueStats(10, 19, 20, 40).compute_rejection_rate() == 0.0
    assert _QueueStats(10, 30, 20, 40).compute_rejection_rate() == 0.5
    assert _QueueStats(10, 40, 20, 40).compute_rejection_rate() == 1.0
    assert _QueueStats(10, 99, 20, 40).compute_rejection_rate() == 1.0


def test_async_acquire_shares_state_with_sync() -> None:
    """One instance, one permit count, whichever world the caller is in."""
    limiter = AdaptiveLimiter(min_limit=1, max_limit=20, initial_limit=1)

    async def scenario() -> None:
        permit = await limiter.acquire_permit_async()
        assert limiter.inflight == 1
        with pytest.raises(LimitExceeded):
            limiter.acquire_permit()
        permit.record()

    asyncio.run(scenario())
    assert limiter.inflight == 0
