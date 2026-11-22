# Events

What listeners receive. Every policy takes its listeners as constructor keywords, one
callable per event.

Guide: [Event listeners](https://failsafe-py.github.io/event-listeners)

The policy-specific events are documented on the page of the policy that emits them.

| Event | Emitted when |
|---|---|
| [`ExecutionEvent`][failsafe.ExecutionEvent] | an attempt is evaluated; the default for most listeners |
| [`ExecutionCompletedEvent`][failsafe.ExecutionCompletedEvent] | an execution finishes, successfully or not |
| [`ScheduledRetryEvent`][failsafe.ScheduledRetryEvent] | a [retry](retry.md) is scheduled, carrying the delay about to elapse |
| [`StateChangedEvent`][failsafe.StateChangedEvent] | the [circuit breaker](circuit-breaker.md) opens, closes or half-opens |
| [`LimitChangedEvent`][failsafe.LimitChangedEvent] | the [adaptive limiter](adaptive-limiter.md) moves its concurrency limit |
| [`BudgetExceededEvent`][failsafe.BudgetExceededEvent] | a [budget](budget.md) refuses a retry or hedge |
| [`ThresholdChangedEvent`][failsafe.ThresholdChangedEvent] | a [prioritizer](priority.md) moves its rejection threshold |
| [`CalibratedEvent`][failsafe.CalibratedEvent] | a [prioritizer](priority.md) finishes a calibration |

::: failsafe.ExecutionEvent

::: failsafe.ExecutionCompletedEvent

::: failsafe.ScheduledRetryEvent
