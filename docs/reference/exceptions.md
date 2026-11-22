# Exceptions

Raised by policies that cannot produce a result. Every one derives from
[`FailsafeError`][failsafe.FailsafeError], so catching that handles any of them.

Each is documented on the page of the policy that raises it.

| Exception | Raised when |
|---|---|
| [`RetriesExceeded`][failsafe.RetriesExceeded] | [Retry](retry.md) runs out of attempts or duration |
| [`CircuitBreakerOpen`][failsafe.CircuitBreakerOpen] | the [circuit breaker](circuit-breaker.md) is open |
| [`TimeoutExceeded`][failsafe.TimeoutExceeded] | a [timeout](timeout.md) elapses |
| [`BulkheadFull`][failsafe.BulkheadFull] | the [bulkhead](bulkhead.md) has no permit free |
| [`RateLimitExceeded`][failsafe.RateLimitExceeded] | the [rate limiter](rate-limiter.md) refuses a permit |
| [`ThrottleExceeded`][failsafe.ThrottleExceeded] | the [adaptive throttler](adaptive-throttler.md) sheds the execution |
| [`LimitExceeded`][failsafe.LimitExceeded] | the [adaptive limiter](adaptive-limiter.md) rejects the execution |
| [`BudgetExceeded`][failsafe.BudgetExceeded] | a [budget](budget.md) refuses a retry |

::: failsafe.FailsafeError
