"""Prioritized rejection, ported from failsafe-go's prioritizer and prioritized tests."""

from __future__ import annotations

import contextlib
import random
import threading

import pytest

from failsafe import (
    AdaptiveLimiter,
    AdaptiveThrottler,
    LimitExceeded,
    Priority,
    ThresholdChangedEvent,
    ThrottleExceeded,
    UsageTracker,
    priority_scope,
    queue_prioritizer,
    throttle_prioritizer,
    user_scope,
)
from failsafe.adaptive_limiter import _Permit, _QueueRejectionStrategy, _QueueStats
from failsafe.adaptive_throttler import _ThrottlerRejectionStrategy, _ThrottlerStats


def test_queue_strategy_sums_every_limiters_queueing() -> None:
    """Go's TestQueueRejectionStrategy_CombineStats, value for value."""
    combined = _QueueRejectionStrategy().combine(
        [
            _QueueStats(limit=5, queued=1, rejection_threshold=8, max_queue=10),
            _QueueStats(limit=15, queued=4, rejection_threshold=20, max_queue=25),
            _QueueStats(limit=25, queued=8, rejection_threshold=35, max_queue=50),
        ]
    )

    assert combined == _QueueStats(limit=45, queued=13, rejection_threshold=63, max_queue=85)


def test_throttler_strategy_is_empty_without_throttlers() -> None:
    combined = _ThrottlerRejectionStrategy().combine([])

    assert combined == _ThrottlerStats(0.0, 0.0, 0.0)


def test_throttler_strategy_passes_a_single_throttler_through() -> None:
    combined = _ThrottlerRejectionStrategy().combine([_ThrottlerStats(100, 0.2, 0.8)])

    assert combined == _ThrottlerStats(100.0, 0.2, 0.8)


def test_throttler_strategy_weights_rates_by_execution_count() -> None:
    """A quiet throttler must not drag a busy one's rate around."""
    combined = _ThrottlerRejectionStrategy().combine(
        [_ThrottlerStats(100, 0.2, 0.8), _ThrottlerStats(100, 0.4, 0.9)]
    )
    assert combined.rejection_rate == pytest.approx(0.3)
    assert combined.max_rejection_rate == 0.8

    lopsided = _ThrottlerRejectionStrategy().combine(
        [_ThrottlerStats(900, 0.1, 0.9), _ThrottlerStats(100, 1.0, 0.9)]
    )
    assert lopsided.rejection_rate == pytest.approx(0.19)


def test_registered_policies_counts_registrations() -> None:
    prioritizer = queue_prioritizer()
    assert prioritizer.registered_policies == 0

    AdaptiveLimiter().prioritized_by(prioritizer)
    AdaptiveLimiter().prioritized_by(prioritizer)

    assert prioritizer.registered_policies == 2


def test_calibrate_computes_a_rate_from_queueing() -> None:
    """Go's TestPrioritizer_Calibrate: three queued, threshold 2, max queue 4."""
    prioritizer = queue_prioritizer()
    limiter = AdaptiveLimiter(
        min_limit=1,
        max_limit=10,
        initial_limit=1,
        queueing=(2.0, 4.0),
    ).prioritized_by(prioritizer)
    queuer = _Queuer()

    held = limiter.acquire_permit(Priority.LOW)
    for _ in range(4):
        queuer.queue(limiter)
    queuer.wait_for_queued(limiter, 4)
    held.record()  # hands the permit to one waiter, leaving three queued
    queuer.wait_for_queued(limiter, 3)

    prioritizer.calibrate()

    assert prioritizer.rejection_rate == 0.5
    assert 0 < prioritizer.rejection_threshold < 200
    queuer.release()


def test_a_full_queue_rejects_everything() -> None:
    prioritizer = queue_prioritizer()
    limiter = AdaptiveLimiter(
        min_limit=1, max_limit=10, initial_limit=1, queueing=(2.0, 4.0)
    ).prioritized_by(prioritizer)
    queuer = _Queuer()

    held = limiter.acquire_permit(Priority.LOW)
    for _ in range(4):
        queuer.queue(limiter)
    queuer.wait_for_queued(limiter, 4)

    prioritizer.calibrate()

    assert prioritizer.rejection_rate == 1.0
    queuer.release(held)


def test_calibrate_is_a_no_op_without_queueing() -> None:
    prioritizer = queue_prioritizer()
    AdaptiveLimiter(min_limit=1, max_limit=10, initial_limit=5).prioritized_by(prioritizer)

    prioritizer.calibrate()

    assert prioritizer.rejection_rate == 0.0
    assert prioritizer.rejection_threshold == 0


def test_threshold_changed_fires_only_on_a_change() -> None:
    """The second calibration computes the same threshold, so it must stay silent."""
    events: list[ThresholdChangedEvent] = []
    prioritizer = queue_prioritizer(on_threshold_changed=events.append)
    limiter = AdaptiveLimiter(
        min_limit=1, max_limit=10, initial_limit=1, queueing=(2.0, 4.0)
    ).prioritized_by(prioritizer)
    queuer = _Queuer()

    held = limiter.acquire_permit(Priority.LOW)
    for _ in range(4):
        queuer.queue(limiter)
    queuer.wait_for_queued(limiter, 4)

    prioritizer.calibrate()
    prioritizer.calibrate()

    assert len(events) == 1
    assert events[0].old_threshold == 0
    assert 100 <= events[0].new_threshold <= 199
    queuer.release(held)


def test_an_execution_above_the_threshold_is_admitted() -> None:
    prioritizer = queue_prioritizer()
    limiter = AdaptiveLimiter().prioritized_by(prioritizer)
    prioritizer._threshold = 200

    assert limiter.acquire_permit(Priority.HIGH) is not None


def test_an_execution_below_the_threshold_is_rejected() -> None:
    prioritizer = queue_prioritizer()
    limiter = AdaptiveLimiter(
        min_limit=1, max_limit=1, initial_limit=1, queueing=(1.0, 1.0)
    ).prioritized_by(prioritizer)
    AdaptiveLimiter().prioritized_by(prioritizer)  # a second policy, so capacity is not consulted
    limiter.acquire_permit(Priority.HIGH)
    prioritizer._threshold = 200

    with pytest.raises(LimitExceeded):
        limiter.acquire_permit(Priority.LOW)


def test_a_sole_limiter_rejects_once_its_queue_is_full() -> None:
    """With one registered policy the limiter need not wait for a calibration."""
    prioritizer = queue_prioritizer()
    limiter = AdaptiveLimiter(
        min_limit=1, max_limit=1, initial_limit=1, queueing=(1.0, 1.0)
    ).prioritized_by(prioritizer)
    held = limiter.acquire_permit(Priority.HIGH)
    queuer = _Queuer()
    queuer.queue(limiter, Priority.HIGH)
    queuer.wait_for_queued(limiter, 1)

    with pytest.raises(LimitExceeded):
        limiter.acquire_permit(Priority.HIGH)

    queuer.release(held)


def test_the_ambient_priority_is_used_when_none_is_passed() -> None:
    """Priority is request-scoped ambient data, not a parameter."""
    prioritizer = queue_prioritizer()
    limiter = AdaptiveLimiter(
        min_limit=1, max_limit=1, initial_limit=1, queueing=(1.0, 1.0)
    ).prioritized_by(prioritizer)
    AdaptiveLimiter().prioritized_by(prioritizer)
    limiter.acquire_permit(Priority.HIGH)
    prioritizer._threshold = 200

    with priority_scope(Priority.LOW), pytest.raises(LimitExceeded):
        limiter.acquire_permit()

    with priority_scope(Priority.VERY_HIGH):
        assert limiter.can_acquire_permit()


def test_accepted_levels_are_recorded_only_while_overloaded() -> None:
    """The level distribution should describe contention, not idle traffic."""
    prioritizer = queue_prioritizer()
    limiter = AdaptiveLimiter(
        min_limit=2, max_limit=2, initial_limit=2, queueing=(2.0, 4.0)
    ).prioritized_by(prioritizer)

    first = limiter.acquire_permit(Priority.HIGH)
    assert prioritizer.level_tracker.get_level(1.0) == 0

    second = limiter.acquire_permit(Priority.HIGH)  # the limiter is now full
    queuer = _Queuer()
    queuer.queue(limiter, Priority.HIGH)
    queuer.wait_for_queued(limiter, 1)

    assert 300 <= prioritizer.level_tracker.get_level(1.0) <= 399

    queuer.release(first, second)


def test_usage_ranking_decides_between_users_at_one_priority() -> None:
    """Go's TestPriorityLimiter_CanAcquirePermit with a usage tracker."""
    usage = UsageTracker(window=60.0, max_users=10)
    prioritizer = queue_prioritizer(usage_tracker=usage)
    limiter = AdaptiveLimiter(
        min_limit=1, max_limit=1, initial_limit=1, queueing=(1.0, 1.0)
    ).prioritized_by(prioritizer)
    AdaptiveLimiter().prioritized_by(prioritizer)
    limiter.acquire_permit(Priority.HIGH)

    usage.record_usage("light", 100)
    usage.record_usage("heavy", 200)
    usage.calibrate()
    prioritizer._threshold = 275

    with user_scope("light"), priority_scope(Priority.MEDIUM):
        assert limiter.can_acquire_permit()

    with user_scope("heavy"), priority_scope(Priority.MEDIUM):
        assert not limiter.can_acquire_permit()


def test_scheduled_calibrations_run_until_the_block_exits() -> None:
    calibrated = threading.Event()
    prioritizer = queue_prioritizer(on_calibrated=lambda _event: calibrated.set())
    limiter = AdaptiveLimiter(
        min_limit=1, max_limit=1, initial_limit=1, queueing=(1.0, 2.0)
    ).prioritized_by(prioritizer)
    queuer = _Queuer()
    held = limiter.acquire_permit(Priority.LOW)
    for _ in range(2):
        queuer.queue(limiter)
    queuer.wait_for_queued(limiter, 2)

    with prioritizer.scheduled_calibrations(0.01):
        assert calibrated.wait(5.0), "no calibration ran"

    assert prioritizer.rejection_rate > 0.0
    assert 100 <= prioritizer.rejection_threshold <= 199
    queuer.release(held)


def test_scheduled_calibrations_rejects_a_non_positive_interval() -> None:
    prioritizer = queue_prioritizer()

    with pytest.raises(ValueError), prioritizer.scheduled_calibrations(0.0):
        pass


def test_a_throttler_admits_levels_above_the_threshold() -> None:
    prioritizer = throttle_prioritizer()
    throttler = AdaptiveThrottler(max_rejection_rate=1.0).prioritized_by(prioritizer)
    prioritizer._threshold = 200

    throttler.acquire_permit(Priority.HIGH)


def test_a_throttler_rejects_levels_below_the_threshold() -> None:
    prioritizer = throttle_prioritizer()
    throttler = AdaptiveThrottler(max_rejection_rate=1.0).prioritized_by(prioritizer)
    prioritizer._threshold = 200

    with pytest.raises(ThrottleExceeded):
        throttler.acquire_permit(Priority.LOW)


def test_a_throttler_lets_a_trickle_through_below_the_threshold() -> None:
    """max_rejection_rate below 1 keeps low priorities from starving entirely."""
    prioritizer = throttle_prioritizer()
    throttler = AdaptiveThrottler(max_rejection_rate=0.5, rng=random.Random(42)).prioritized_by(
        prioritizer
    )
    prioritizer._threshold = 400

    admitted = sum(throttler.try_acquire_permit(Priority.VERY_LOW) for _ in range(1000))

    assert 400 < admitted < 600


def test_an_unprioritized_throttler_is_unchanged() -> None:
    """Prioritization is opt-in; the default path must keep rejecting at random."""
    throttler = AdaptiveThrottler(rng=random.Random(42))

    assert throttler.try_acquire_permit()


class _Queuer:
    """Queues executions that hold their permits until released, as Go's tests do.

    Recording a permit immediately would drain the queue before the assertion runs.
    """

    def __init__(self) -> None:
        self._release = threading.Event()
        self._threads: list[threading.Thread] = []

    def queue(self, limiter: AdaptiveLimiter, priority: Priority = Priority.LOW) -> None:
        def acquire() -> None:
            with contextlib.suppress(LimitExceeded):
                permit = limiter.acquire_permit(priority)
                self._release.wait(5.0)
                permit.record()

        thread = threading.Thread(target=acquire, daemon=True)
        thread.start()
        self._threads.append(thread)

    def wait_for_queued(self, limiter: AdaptiveLimiter, count: int) -> None:
        assert limiter.await_queued(count), f"only {limiter.queued} of {count} queued"

    def release(self, *held: _Permit) -> None:
        """Release the queue, then any permits the test itself is holding.

        Queued threads block inside ``acquire_permit``, so they only reach their own
        release once a permit frees up; dropping the held ones is what drains them.
        """
        self._release.set()
        for permit in held:
            permit.record()
        for thread in self._threads:
            thread.join(5.0)
            assert not thread.is_alive(), "a queued execution never finished"
