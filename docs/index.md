# failsafe-py API reference

Generated from the docstrings in `failsafe`, a port of
[failsafe-go](https://github.com/failsafe-go/failsafe-go).

This site is the reference. The prose guide — concepts, usage, comparisons — lives at
[failsafe-py.github.io](https://failsafe-py.github.io).

## Shape of the library

A composition wraps a call with policies, outermost first:

```python
from failsafe import CircuitBreaker, Failsafe, RetryPolicy, Timeout

api = Failsafe(RetryPolicy(max_retries=3), CircuitBreaker(), Timeout(10.0))
response = api.call(send_request, url)
```

The result type comes from the function, and its arguments are checked against that
function's signature, so one composition can guard calls returning different types.

Stateful policies — `CircuitBreaker`, `Bulkhead`, `RateLimiter`, `AdaptiveThrottler`,
`AdaptiveLimiter`, `Budget` — model the health of a *dependency*, not of a call site.
Construct one per dependency and share it; building one per call site splits the signal
that makes them work.

| | |
|---|---|
| [Entry points](reference/entry-points.md) | `Failsafe`, `AsyncFailsafe`, and the execution in progress |
| [Policies](reference/retry.md) | retry, circuit breaker, timeout, fallback, bulkhead, rate limiter, hedge, cache, budget, adaptive throttler and limiter |
| [Vocabulary](reference/conditions.md) | conditions, delays, priority, events, exceptions |
