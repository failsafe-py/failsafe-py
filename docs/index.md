# failsafe-py API reference

This is the docstring-generated API reference. The prose guide — concepts, usage,
comparisons — lives at [failsafe-py.github.io](https://failsafe-py.github.io).

## Shape of the library

A composition wraps a call with policies, outermost first:

```python
from failsafe import CircuitBreaker, Failsafe, RetryPolicy, Timeout

api = Failsafe(RetryPolicy(max_retries=3), CircuitBreaker(), Timeout(2.0))
response = api.call(send_request, url)
```

The result type comes from the function, and its arguments are checked against that
function's signature, so one composition can guard calls returning different types.

Stateful policies — `CircuitBreaker`, `Bulkhead`, `RateLimiter`, `AdaptiveThrottler`,
`AdaptiveLimiter`, `Budget` — accumulate state across executions, so they may be
shared. What a shared instance then measures is whatever its callers have in common:
one per backend tracks that backend's health, one per connection or per operation
tracks something narrower. Pick the scope you want to reason about.

| | |
|---|---|
| [Entry points](reference/entry-points.md) | `Failsafe`, `AsyncFailsafe`, and the execution in progress |
| [Policies](reference/retry.md) | retry, circuit breaker, timeout, fallback, bulkhead, rate limiter, hedge, cache, budget, adaptive throttler and limiter |
| [Vocabulary](reference/conditions.md) | conditions, delays, priority, events, exceptions |
