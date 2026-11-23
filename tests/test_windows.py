from __future__ import annotations

import math
import random

import pytest

from failsafe._windows import (
    CorrelationWindow,
    MaxWindow,
    MovingAverage,
    MovingMedian,
    QuantileWindow,
    log10_step,
    smooth,
)


def test_smooth_blends_towards_the_new_value() -> None:
    assert smooth(0.0, 10.0, 0.5) == 5.0
    assert smooth(10.0, 20.0, 0.1) == pytest.approx(11.0)


@pytest.mark.parametrize(
    ("limit", "expected"),
    [(1, 3), (99, 3), (100, 6), (999, 6), (1000, 9), (10000, 12)],
)
def test_log10_step_scales_with_limit_magnitude(limit: int, expected: int) -> None:
    assert log10_step(3, limit) == expected


def test_moving_average_uses_a_simple_mean_during_warmup() -> None:
    average = MovingAverage(age=10, warmup_samples=3)

    assert average.add(10.0) == 10.0
    assert average.add(20.0) == 15.0
    assert average.add(30.0) == 20.0


def test_moving_average_decays_after_warmup() -> None:
    """Past warmup the value is an EMA, so it tracks but lags a step change."""
    average = MovingAverage(age=9, warmup_samples=1)
    average.add(0.0)

    first = average.add(10.0)

    assert first == pytest.approx(2.0)  # smoothing factor 2/(9+1)
    assert average.add(10.0) == pytest.approx(3.6)


def test_moving_average_reset_requires_a_new_warmup() -> None:
    average = MovingAverage(age=10, warmup_samples=2)
    average.add(100.0)
    average.reset()

    assert average.value == 0.0
    assert average.add(5.0) == 5.0


def test_moving_median_returns_the_value_until_the_window_fills() -> None:
    median = MovingMedian(3)

    assert median.add(10.0) == 10.0
    assert median.add(30.0) == 30.0
    assert median.median() == 0.0


def test_moving_median_ignores_an_outlier_once_full() -> None:
    """The point of the filter: one freak sample must not move the limit."""
    median = MovingMedian(3)
    median.add(10.0)
    median.add(11.0)

    assert median.add(900.0) == 11.0


def test_moving_median_evicts_the_oldest_value() -> None:
    median = MovingMedian(3)
    for value in (1.0, 2.0, 3.0):
        median.add(value)

    assert median.add(100.0) == 3.0


def test_quantile_window_is_exact_for_known_samples() -> None:
    window = QuantileWindow()
    for value in range(1, 101):
        window.add(float(value), inflight=1)

    assert window.quantile(0.5) == pytest.approx(50.5)
    assert window.quantile(0.9) == pytest.approx(90.1)
    assert window.quantile(0.0) == 1.0
    assert window.quantile(1.0) == 100.0


def test_quantile_window_matches_numpy_style_interpolation() -> None:
    """Exactness is the reason this replaces a t-digest; pin it to a hand computation."""
    window = QuantileWindow()
    for value in (10.0, 20.0, 30.0, 40.0):
        window.add(value, inflight=1)

    # position = 0.9 * 3 = 2.7, so 30 + 0.7 * (40 - 30)
    assert window.quantile(0.9) == pytest.approx(37.0)


def test_quantile_window_tracks_min_rtt_and_max_inflight() -> None:
    window = QuantileWindow()
    window.add(5.0, inflight=3)
    window.add(2.0, inflight=9)
    window.add(8.0, inflight=1)

    assert window.min_rtt == 2.0
    assert window.max_inflight == 9
    assert window.size == 3


def test_quantile_window_reset_clears_everything() -> None:
    window = QuantileWindow()
    window.add(5.0, inflight=3)
    window.reset()

    assert window.size == 0
    assert window.min_rtt == 0.0
    assert window.max_inflight == 0
    assert window.quantile(0.5) == 0.0


def test_correlation_window_detects_a_positive_correlation() -> None:
    window = CorrelationWindow(capacity=20, warmup_samples=3)
    correlation = 0.0
    for i in range(1, 21):
        correlation, _, _ = window.add(float(i), float(i) * 2)

    assert correlation == pytest.approx(1.0, abs=1e-9)


def test_correlation_window_detects_a_negative_correlation() -> None:
    """The signal that matters: throughput falling as concurrency rises."""
    window = CorrelationWindow(capacity=20, warmup_samples=3)
    correlation = 0.0
    for i in range(1, 21):
        correlation, _, _ = window.add(float(i), float(100 - i * 2))

    assert correlation == pytest.approx(-1.0, abs=1e-9)


def test_correlation_window_is_zero_during_warmup() -> None:
    window = CorrelationWindow(capacity=20, warmup_samples=10)

    for i in range(1, 6):
        correlation, _, _ = window.add(float(i), float(i))
        assert correlation == 0.0


def test_correlation_window_ignores_series_that_barely_vary() -> None:
    window = CorrelationWindow(capacity=20, warmup_samples=3)
    correlation = 1.0
    for i in range(20):
        correlation, _, _ = window.add(100.0 + i * 0.0001, 200.0 + i * 0.0001)

    assert correlation == 0.0


def test_correlation_window_ignores_infinities() -> None:
    window = CorrelationWindow(capacity=5, warmup_samples=1)

    assert window.add(math.inf, 1.0) == (0.0, 0.0, 0.0)
    assert window.add(1.0, math.inf) == (0.0, 0.0, 0.0)


def test_correlation_window_reset_clears_the_series() -> None:
    window = CorrelationWindow(capacity=5, warmup_samples=1)
    for i in range(5):
        window.add(float(i + 1), float(i + 1))
    window.reset()

    correlation, _, _ = window.add(1.0, 1.0)
    assert correlation == 0.0


def test_max_window_is_unconfigured_at_zero() -> None:
    window = MaxWindow(0.0)

    assert not window.configured
    assert window.value == 0


def test_max_window_holds_the_peak_across_the_window() -> None:
    window = MaxWindow(10.0)

    assert window.add(20, now=0.0) == 20
    assert window.add(5, now=1.0) == 20  # dip does not lower the max
    assert window.add(5, now=2.0) == 20


def test_max_window_drops_the_peak_once_it_expires() -> None:
    window = MaxWindow(10.0)
    window.add(20, now=0.0)

    assert window.add(5, now=11.0) == 5


def test_max_window_reset_empties_it() -> None:
    window = MaxWindow(10.0)
    window.add(20, now=0.0)
    window.reset()

    assert window.value == 0


def test_quantile_window_beats_a_sketch_at_limiter_sample_counts() -> None:
    """The exactness claim, as a check rather than an assertion in prose.

    A t-digest carries percent-level error on a p90 at these window sizes; the sorted
    window is exact by construction. This pins the exactness that justifies the choice.
    """
    rng = random.Random(42)
    samples = [math.exp(rng.gauss(0, 0.6)) * 100 for _ in range(50)]
    window = QuantileWindow()
    for sample in samples:
        window.add(sample, inflight=1)

    ordered = sorted(samples)
    position = 0.9 * (len(ordered) - 1)
    lower = int(position)
    expected = ordered[lower] + (ordered[lower + 1] - ordered[lower]) * (position - lower)

    assert window.quantile(0.9) == pytest.approx(expected, abs=1e-12)
