from __future__ import annotations

import random

import pytest

from failsafe import LevelTracker, Priority, UsageTracker, level_scope, priority_scope, user_scope
from failsafe._core import FakeClock
from failsafe.priority import TOTAL_LEVELS, current_level, current_priority, current_user


def test_each_priority_owns_a_hundred_levels() -> None:
    assert Priority.VERY_LOW.min_level == 0
    assert Priority.VERY_LOW.max_level == 99
    assert Priority.HIGH.min_level == 300
    assert Priority.HIGH.max_level == 399
    assert Priority.VERY_HIGH.max_level == TOTAL_LEVELS - 1


def test_random_level_stays_within_the_priority() -> None:
    rng = random.Random(42)
    for _ in range(200):
        assert 300 <= Priority.HIGH.random_level(rng) <= 399


def test_priority_scope_sets_and_restores_the_ambient_priority() -> None:
    assert current_priority() is None

    with priority_scope(Priority.HIGH):
        assert current_priority() is Priority.HIGH
        with priority_scope(Priority.LOW):
            assert current_priority() is Priority.LOW
        assert current_priority() is Priority.HIGH

    assert current_priority() is None


def test_current_level_derives_a_level_from_the_priority_in_scope() -> None:
    with priority_scope(Priority.MEDIUM):
        level = current_level(random.Random(42))

    assert level is not None
    assert 200 <= level <= 299


def test_an_exact_level_wins_over_the_priority() -> None:
    """level_scope is how a caller pins a level rather than sampling one."""
    with priority_scope(Priority.VERY_LOW), level_scope(450):
        assert current_level() == 450


def test_current_level_is_none_without_a_scope() -> None:
    assert current_level() is None


def test_level_scope_rejects_out_of_range_levels() -> None:
    with pytest.raises(ValueError), level_scope(TOTAL_LEVELS):
        pass
    with pytest.raises(ValueError), level_scope(-1):
        pass


def test_user_scope_sets_the_ambient_user() -> None:
    assert current_user() is None

    with user_scope("user1"):
        assert current_user() == "user1"

    assert current_user() is None


def test_level_tracker_returns_zero_before_anything_is_recorded() -> None:
    assert LevelTracker().get_level(0.5) == 0


def test_level_tracker_finds_the_level_at_a_quantile() -> None:
    tracker = LevelTracker(window_size=100)
    for level in range(100, 200):
        tracker.record_level(level)

    assert tracker.get_level(0.0) == 100
    assert tracker.get_level(0.5) == 149
    assert tracker.get_level(1.0) == 199


def test_level_tracker_evicts_beyond_its_window() -> None:
    """A stale level must not keep influencing the threshold forever."""
    tracker = LevelTracker(window_size=2)
    tracker.record_level(10)
    tracker.record_level(400)
    tracker.record_level(400)

    assert tracker.get_level(0.5) == 400


def test_level_tracker_reset_clears_the_window() -> None:
    tracker = LevelTracker(window_size=10)
    tracker.record_level(150)
    tracker.reset()

    assert tracker.get_level(0.5) == 0


def test_level_tracker_rejects_an_empty_window() -> None:
    with pytest.raises(ValueError):
        LevelTracker(window_size=0)


def test_usage_tracker_totals_usage_per_user() -> None:
    tracker = UsageTracker(window=60.0, max_users=10)

    tracker.record_usage("user1", 100)
    tracker.record_usage("user1", 50)
    tracker.record_usage("user2", 200)

    assert tracker.get_usage("user1") == 150
    assert tracker.get_usage("user2") == 200
    assert tracker.get_usage("unknown") is None


def test_an_unknown_user_gets_the_priority_top_level() -> None:
    tracker = UsageTracker(window=60.0, max_users=10)

    assert tracker.get_level("unknown", Priority.HIGH) == Priority.HIGH.max_level


def test_an_uncalibrated_user_gets_the_priority_top_level() -> None:
    """Usage counts against a user only once it has been ranked."""
    tracker = UsageTracker(window=60.0, max_users=10)
    tracker.record_usage("user1", 100)

    assert tracker.get_level("user1", Priority.HIGH) == Priority.HIGH.max_level


def test_heavier_users_rank_below_lighter_ones() -> None:
    tracker = UsageTracker(window=60.0, max_users=10)
    tracker.record_usage("light", 50)
    tracker.record_usage("medium", 100)
    tracker.record_usage("heavy", 200)

    tracker.calibrate()

    light = tracker.get_level("light", Priority.MEDIUM)
    medium = tracker.get_level("medium", Priority.MEDIUM)
    heavy = tracker.get_level("heavy", Priority.MEDIUM)

    assert light > heavy
    assert medium >= heavy
    assert light >= medium


def test_levels_stay_within_the_requested_priority() -> None:
    tracker = UsageTracker(window=60.0, max_users=10)
    tracker.record_usage("user1", 100)
    tracker.calibrate()

    for priority in Priority:
        level = tracker.get_level("user1", priority)
        assert priority.min_level <= level <= priority.max_level


def test_usage_tracker_evicts_the_least_recently_active_user() -> None:
    tracker = UsageTracker(window=60.0, max_users=2)
    tracker.record_usage("user1", 100)
    tracker.record_usage("user2", 200)

    tracker.record_usage("user3", 300)

    assert tracker.get_usage("user1") is None
    assert tracker.get_usage("user2") == 200
    assert tracker.get_usage("user3") == 300


def test_usage_ages_out_of_the_window() -> None:
    """Usage is a rolling window, so an hour-old spike stops counting."""
    clock = FakeClock()
    tracker = UsageTracker(window=100.0, max_users=10, clock=clock)
    tracker.record_usage("user1", 100)
    assert tracker.get_usage("user1") == 100

    clock.advance(101.0)
    tracker.calibrate()

    assert tracker.get_usage("user1") == 0


def test_long_idle_users_are_forgotten() -> None:
    clock = FakeClock()
    tracker = UsageTracker(window=100.0, max_users=10, clock=clock)
    tracker.record_usage("user1", 100)

    clock.advance(201.0)
    tracker.calibrate()

    assert tracker.get_usage("user1") is None


def test_an_idle_user_reverts_to_the_top_level() -> None:
    """Once usage ages out there is nothing to penalise them for."""
    clock = FakeClock()
    tracker = UsageTracker(window=100.0, max_users=10, clock=clock)
    tracker.record_usage("light", 100)
    tracker.record_usage("heavy", 900)
    tracker.calibrate()
    assert tracker.get_level("heavy", Priority.MEDIUM) < Priority.MEDIUM.max_level

    clock.advance(101.0)
    tracker.calibrate()

    assert tracker.get_level("heavy", Priority.MEDIUM) == Priority.MEDIUM.max_level


def test_usage_tracker_rejects_invalid_configuration() -> None:
    with pytest.raises(ValueError):
        UsageTracker(window=0.0)
    with pytest.raises(ValueError):
        UsageTracker(max_users=0)
