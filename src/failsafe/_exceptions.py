"""Exceptions raised by policies when they cannot produce a result."""

from __future__ import annotations

from typing import Any

__all__ = [
    "CircuitBreakerOpen",
    "FailsafeError",
    "RetriesExceeded",
    "ThrottleExceeded",
    "TimeoutExceeded",
]


class FailsafeError(Exception):
    """Base class for every exception a policy raises itself.

    Catch this to handle any policy-level failure without naming each one.
    """


class RetriesExceeded(FailsafeError):
    """A :class:`~failsafe.RetryPolicy` ran out of attempts or exceeded its duration.

    Raised instead of the last failure unless the policy was built with
    ``return_last_failure=True``. The last failure is kept on the exception, and
    chained as ``__cause__`` when it was an exception, so ``raise ... from`` traceback
    context survives.

    Attributes:
        last_result: The value returned by the final attempt, if it returned one.
        last_exception: The exception raised by the final attempt, if it raised one.
    """

    def __init__(self, last_result: Any = None, last_exception: Exception | None = None) -> None:
        super().__init__(
            f"retries exceeded (last result: {last_result!r}, last exception: {last_exception!r})"
        )
        self.last_result = last_result
        self.last_exception = last_exception


class CircuitBreakerOpen(FailsafeError):
    """A :class:`~failsafe.CircuitBreaker` rejected an execution without running it.

    Raised when the breaker is open, or is half-open and already admitting as many
    trial executions as it permits.

    Attributes:
        remaining_delay: Seconds until the breaker will admit a trial execution, or
            ``0.0`` when it is already willing to.
    """

    def __init__(self, remaining_delay: float = 0.0) -> None:
        super().__init__(f"circuit breaker open (retry in {remaining_delay:.3f}s)")
        self.remaining_delay = remaining_delay


class TimeoutExceeded(FailsafeError):
    """A :class:`~failsafe.Timeout` elapsed before the execution finished.

    An ordinary failure: an outer :class:`~failsafe.RetryPolicy` may retry it, unlike
    an :exc:`asyncio.CancelledError`, which is terminal (PLAN section 8 item 6).

    Attributes:
        timeout: The configured limit in seconds.
    """

    def __init__(self, timeout: float) -> None:
        super().__init__(f"timeout exceeded after {timeout}s")
        self.timeout = timeout


class ThrottleExceeded(FailsafeError):
    """An :class:`~failsafe.AdaptiveThrottler` rejected an execution without running it.

    Rejection is probabilistic: an identical call a moment later may be admitted, since
    the throttler always lets some traffic through to probe for recovery.

    Attributes:
        rejection_rate: The share of executions being rejected when this one was, from
            ``0`` to ``1``.
    """

    def __init__(self, rejection_rate: float = 0.0) -> None:
        super().__init__(f"throttled (rejection rate: {rejection_rate:.3f})")
        self.rejection_rate = rejection_rate
