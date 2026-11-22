"""Fault tolerance and resilience policies for Python.

A port of failsafe-go.
"""

from ._async import AsyncFailsafe
from ._conditions import Condition
from ._core import Execution, current_execution
from ._delays import Backoff, Delay, Fixed, Jitter, RandomDelay
from ._events import (
    ExecutionCompletedEvent,
    ExecutionEvent,
    ScheduledRetryEvent,
    StateChangedEvent,
)
from ._exceptions import (
    BudgetExceeded,
    BulkheadFull,
    CircuitBreakerOpen,
    FailsafeError,
    LimitExceeded,
    RateLimitExceeded,
    RetriesExceeded,
    ThrottleExceeded,
    TimeoutExceeded,
)
from ._prioritizer import CalibratedEvent, Prioritizer, ThresholdChangedEvent
from ._sync import Failsafe
from .adaptive_limiter import AdaptiveLimiter, LimitChangedEvent, queue_prioritizer
from .adaptive_throttler import AdaptiveThrottler, throttle_prioritizer
from .budget import Budget, BudgetExceededEvent
from .bulkhead import Bulkhead
from .cache import Cache, CachePolicy, cache_key, current_cache_key
from .circuit_breaker import CircuitBreaker, Rate, Ratio
from .fallback import Fallback
from .hedge import HedgePolicy
from .priority import (
    LevelTracker,
    Priority,
    UsageTracker,
    current_level,
    current_user,
    level_scope,
    priority_scope,
    user_scope,
)
from .rate_limiter import RateLimiter
from .retry import RetryPolicy
from .timeout import Timeout

__all__ = [
    "AdaptiveLimiter",
    "AdaptiveThrottler",
    "AsyncFailsafe",
    "Backoff",
    "Budget",
    "BudgetExceeded",
    "BudgetExceededEvent",
    "Bulkhead",
    "BulkheadFull",
    "Cache",
    "CachePolicy",
    "CalibratedEvent",
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
    "HedgePolicy",
    "Jitter",
    "LevelTracker",
    "LimitChangedEvent",
    "LimitExceeded",
    "Prioritizer",
    "Priority",
    "RandomDelay",
    "Rate",
    "RateLimitExceeded",
    "RateLimiter",
    "Ratio",
    "RetriesExceeded",
    "RetryPolicy",
    "ScheduledRetryEvent",
    "StateChangedEvent",
    "ThresholdChangedEvent",
    "ThrottleExceeded",
    "Timeout",
    "TimeoutExceeded",
    "UsageTracker",
    "cache_key",
    "current_cache_key",
    "current_execution",
    "current_level",
    "current_user",
    "level_scope",
    "priority_scope",
    "queue_prioritizer",
    "throttle_prioritizer",
    "user_scope",
]
