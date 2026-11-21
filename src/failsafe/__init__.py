"""Fault tolerance and resilience policies for Python.

A port of failsafe-go. See PLAN.md for the design and PORT.md for differences
from the Go library.
"""

from ._async import AsyncFailsafe
from ._conditions import Condition, Result
from ._core import Execution, current_execution
from ._delays import Backoff, Delay, Fixed, Jitter, RandomDelay
from ._events import (
    ExecutionCompletedEvent,
    ExecutionEvent,
    ScheduledRetryEvent,
    StateChangedEvent,
)
from ._exceptions import (
    CircuitBreakerOpen,
    FailsafeError,
    RetriesExceeded,
    TimeoutExceeded,
)
from ._sync import Failsafe
from .circuit_breaker import CircuitBreaker, Rate, Ratio
from .fallback import Fallback
from .retry import RetryPolicy
from .timeout import Timeout

__all__ = [
    "AsyncFailsafe",
    "Backoff",
    "CircuitBreaker",
    "CircuitBreakerOpen",
    "Condition",
    "Delay",
    "Execution",
    "ExecutionCompletedEvent",
    "ExecutionEvent",
    "Failsafe",
    "FailsafeError",
    "Fallback",
    "Fixed",
    "Jitter",
    "RandomDelay",
    "Rate",
    "Ratio",
    "Result",
    "RetriesExceeded",
    "RetryPolicy",
    "ScheduledRetryEvent",
    "StateChangedEvent",
    "Timeout",
    "TimeoutExceeded",
    "current_execution",
]
