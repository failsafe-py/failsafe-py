"""Rolling statistical windows, ported from failsafe-go's ``internal/util``.

These back the [`AdaptiveLimiter`][failsafe.AdaptiveLimiter]'s view of execution-time trends.
None are concurrency safe; the owning policy holds its lock.
"""

from __future__ import annotations

import bisect
import collections
import math

__all__ = [
    "CorrelationWindow",
    "MaxWindow",
    "MovingAverage",
    "MovingMedian",
    "MovingQuantile",
    "QuantileWindow",
    "log10_step",
    "smooth",
]


def smooth(old_value: float, new_value: float, factor: float) -> float:
    """Blend ``new_value`` into ``old_value`` by ``factor``, as Go's ``util.Smooth``."""
    return old_value * (1 - factor) + new_value * factor


def log10_step(factor: int, limit: int) -> int:
    """The step size for a limit change, as Go's ``util.Log10Func``.

    The limit moves by ``factor`` for limits below 100, ``2 * factor`` below 1000, and
    ``factor * floor(log10(limit))`` above that, so large limits move in larger steps.

    Args:
        factor: The base step size.
        limit: The current concurrency limit.

    Returns:
        How far the limit should move.
    """
    if limit < 100:
        return factor
    if limit < 1000:
        return factor * 2
    return factor * int(math.log10(limit))


class MovingAverage:
    """An exponentially weighted moving average, as Go's ``util.MovingAverage``.

    A simple mean is used for the first ``warmup_samples`` values, after which
    exponential decay takes over.

    Args:
        age: How far back the average effectively remembers. Smaller ages adapt faster.
        warmup_samples: Values averaged simply before decay begins.
    """

    __slots__ = ("_count", "_smoothing_factor", "_sum", "_warmup_samples", "value")

    def __init__(self, age: int, warmup_samples: int) -> None:
        self._smoothing_factor = 2 / (age + 1)
        self._warmup_samples = warmup_samples
        self._count = 0
        self._sum = 0.0
        self.value = 0.0

    def add(self, value: float) -> float:
        """Add a value and return the updated average."""
        if self._count < self._warmup_samples:
            self._count += 1
            self._sum += value
            self.value = self._sum / self._count
        else:
            self.value = smooth(self.value, value, self._smoothing_factor)
        return self.value

    def reset(self) -> None:
        """Clear the average and require a new warmup."""
        self._count = 0
        self._sum = 0.0
        self.value = 0.0


class MovingMedian:
    """The median over the last ``size`` values, as Go's ``util.MovingMedian``.

    Args:
        size: How many values the window holds.
    """

    __slots__ = ("_size", "_values")

    def __init__(self, size: int) -> None:
        self._size = size
        self._values: collections.deque[float] = collections.deque(maxlen=size)

    def add(self, value: float) -> float:
        """Add a value and return the current median, or ``value`` while filling."""
        self._values.append(value)
        if len(self._values) < self._size:
            return value
        return self.median()

    def median(self) -> float:
        """The current median, or ``0`` before the window has filled."""
        if len(self._values) < self._size:
            return 0.0
        return sorted(self._values)[self._size // 2]

    def reset(self) -> None:
        """Empty the window."""
        self._values.clear()


class MovingQuantile:
    """A streaming quantile estimate, as Go's ``util.MovingQuantile``.

    The Windowless Moving Percentile algorithm: O(1) in time and space, holding no
    samples. The estimate steps toward each new sample by a fraction of the running
    standard deviation, so it tracks a shifting distribution rather than averaging
    over all history.

    Distinct from `QuantileWindow`, which keeps its samples and is exact.
    This one is for the hedge policy, where samples arrive without bound and only
    the current estimate matters.

    Args:
        quantile: The quantile to estimate, between 0 and 1 exclusive.
        r: The step ratio applied to the standard deviation.
        age: How many samples the estimate effectively remembers. Smaller ages adapt
            faster; larger ages are steadier.
    """

    __slots__ = ("_alpha", "_mean", "_quantile", "_r", "_variance", "count", "value")

    def __init__(self, quantile: float, r: float, age: int) -> None:
        self._quantile = quantile
        self._r = r
        self._alpha = 2 / (age + 1)
        self._mean = 0.0
        self._variance = 0.0
        self.count = 0
        """How many samples have been added."""
        self.value = 0.0
        """The current quantile estimate."""

    def add(self, sample: float) -> float:
        """Add a sample and return the updated estimate."""
        self.count += 1
        if self.count == 1:
            self.value = sample
            self._mean = sample
            return self.value

        old_mean = self._mean
        self._mean = smooth(self._mean, sample, self._alpha)
        self._variance = smooth(
            self._variance, (sample - old_mean) * (sample - self._mean), self._alpha
        )

        delta = math.sqrt(self._variance) * self._r
        if delta == 0:
            return self.value
        if sample < self.value:
            self.value -= delta / self._quantile
        elif sample > self.value:
            self.value += delta / (1 - self._quantile)
        return self.value

    def reset(self) -> None:
        """Clear the estimate and the sample count."""
        self._mean = 0.0
        self._variance = 0.0
        self.count = 0
        self.value = 0.0


class QuantileWindow:
    """Exact quantiles over the samples in the current window.

    Replaces Go's ``influxdata/tdigest`` in the adaptive limiter. The limiter resets
    this window every time it updates its limit, so it holds tens to hundreds of
    samples — small enough that a sorted insert is both cheaper and exact, where a
    t-digest only approximates.
    """

    __slots__ = ("_samples", "max_inflight", "min_rtt")

    def __init__(self) -> None:
        self._samples: list[float] = []
        self.min_rtt = 0.0
        self.max_inflight = 0

    def add(self, rtt: float, inflight: int) -> None:
        """Record an execution's round-trip time and the inflight count it saw."""
        if not self._samples:
            self.min_rtt = rtt
            self.max_inflight = inflight
        else:
            self.min_rtt = min(self.min_rtt, rtt)
            self.max_inflight = max(self.max_inflight, inflight)
        bisect.insort(self._samples, rtt)

    @property
    def size(self) -> int:
        """How many samples the window holds."""
        return len(self._samples)

    def quantile(self, quantile: float) -> float:
        """The sample at ``quantile``, interpolating between neighbours.

        Args:
            quantile: The quantile to compute, from ``0`` to ``1``.

        Returns:
            The interpolated sample, or ``0`` if the window is empty.
        """
        if not self._samples:
            return 0.0
        if len(self._samples) == 1:
            return self._samples[0]
        position = quantile * (len(self._samples) - 1)
        lower = int(position)
        upper = min(lower + 1, len(self._samples) - 1)
        lower_value = self._samples[lower]
        return lower_value + (self._samples[upper] - lower_value) * (position - lower)

    def reset(self) -> None:
        """Drop every sample, starting a new window."""
        self._samples.clear()
        self.min_rtt = 0.0
        self.max_inflight = 0


class _MovingSum:
    """A rolling sum and sum-of-squares over the last ``capacity`` non-zero values."""

    __slots__ = ("_capacity", "_index", "_samples", "_sum", "_sum_squares", "size")

    def __init__(self, capacity: int) -> None:
        self._capacity = capacity
        self._samples = [0.0] * capacity
        self._index = 0
        self._sum = 0.0
        self._sum_squares = 0.0
        self.size = 0

    def add(self, value: float) -> tuple[float, bool]:
        """Add a non-zero value, returning the evicted value and whether it was full."""
        old_value = 0.0
        full = False
        if value != 0:
            if self.size == self._capacity:
                full = True
                old_value = self._samples[self._index]
                self._sum -= old_value
                self._sum_squares -= old_value * old_value
            else:
                self.size += 1
            self._samples[self._index] = value
            self._sum += value
            self._sum_squares += value * value
            self._index = (self._index + 1) % self._capacity
        return old_value, full

    def coefficient_of_variation(self) -> tuple[float, float, float] | None:
        """The CV, mean and variance, or ``None`` when they are undefined."""
        if self.size < 2:
            return None
        mean = self._sum / self.size
        variance = (self._sum_squares / self.size) - (mean * mean)
        if variance < 0 or mean == 0:
            return None
        return math.sqrt(variance) / mean, mean, variance

    def reset(self) -> None:
        """Clear the window."""
        self._samples = [0.0] * self._capacity
        self._index = 0
        self._sum = 0.0
        self._sum_squares = 0.0
        self.size = 0


_MIN_CV = 0.01


class CorrelationWindow:
    """The correlation between two rolling series, as Go's ``util.CorrelationWindow``.

    The limiter uses this to tell a healthy concurrency increase (throughput rises with
    inflight) from an overloaded one (latency rises, throughput does not).

    Args:
        capacity: How many pairs the window holds.
        warmup_samples: Pairs required before a correlation is reported.
    """

    __slots__ = ("_corr_sum_xy", "_warmup_samples", "_x", "_y")

    def __init__(self, capacity: int, warmup_samples: int) -> None:
        self._warmup_samples = warmup_samples
        self._x = _MovingSum(capacity)
        self._y = _MovingSum(capacity)
        self._corr_sum_xy = 0.0

    def add(self, x: float, y: float) -> tuple[float, float, float]:
        """Add a pair and return the correlation with each series' coefficient of variation.

        Returns ``1`` to ``-1`` as the series correlate positively or negatively, and
        ``0`` during warmup or when either series varies by less than 1% — too little
        movement to read a correlation from.

        Args:
            x: The first series' value.
            y: The second series' value.

        Returns:
            A ``(correlation, cv_x, cv_y)`` triple.
        """
        if math.isinf(x) or math.isinf(y):
            return 0.0, 0.0, 0.0

        old_x, full = self._x.add(x)
        old_y, _ = self._y.add(y)
        stats_x = self._x.coefficient_of_variation()
        stats_y = self._y.coefficient_of_variation()

        if full:
            self._corr_sum_xy -= old_x * old_y
        self._corr_sum_xy += x * y

        if stats_x is None or stats_y is None:
            return 0.0, 0.0, 0.0
        cv_x, mean_x, var_x = stats_x
        cv_y, mean_y, var_y = stats_y

        if self._x.size < self._warmup_samples:
            return 0.0, 0.0, 0.0
        if cv_x < _MIN_CV or cv_y < _MIN_CV:
            return 0.0, cv_x, cv_y

        covariance = (self._corr_sum_xy / self._x.size) - (mean_x * mean_y)
        return covariance / (math.sqrt(var_x) * math.sqrt(var_y)), cv_x, cv_y

    def reset(self) -> None:
        """Clear both series."""
        self._x.reset()
        self._y.reset()
        self._corr_sum_xy = 0.0


class MaxWindow:
    """The maximum value over a sliding time window, as Go's ``util.MaxWindow``.

    Backed by a monotonic deque, so both insertion and lookup are O(1) amortised.

    Args:
        window: The span in seconds over which to keep a maximum. ``0`` disables it.
    """

    __slots__ = ("_entries", "_window")

    def __init__(self, window: float) -> None:
        self._window = window
        self._entries: collections.deque[tuple[int, float]] = collections.deque()

    @property
    def configured(self) -> bool:
        """Whether a non-zero window was given."""
        return self._window > 0

    def add(self, value: int, now: float) -> int:
        """Add a value at ``now`` and return the current maximum."""
        cutoff = now - self._window
        while self._entries and self._entries[0][1] <= cutoff:
            self._entries.popleft()
        while self._entries and self._entries[-1][0] <= value:
            self._entries.pop()
        self._entries.append((value, now))
        return self.value

    @property
    def value(self) -> int:
        """The current maximum, or ``0`` when unconfigured or empty."""
        if not self.configured or not self._entries:
            return 0
        return self._entries[0][0]

    def reset(self) -> None:
        """Empty the window."""
        self._entries.clear()
