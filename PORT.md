# Porting from failsafe-go

For people arriving from [failsafe-go](https://github.com/failsafe-go/failsafe-go) or
[failsafe](https://failsafe.dev) (Java). If you have used neither, you do not need this file —
start at the documentation site.

## Name mapping

| failsafe-go | failsafe-py |
|---|---|
| `failsafe.With(p1, p2).Get(fn)` | `Failsafe(p1, p2).call(fn)` |
| `failsafe.With(...).RunAsync(fn)` | `asyncio.create_task(AsyncFailsafe(...).call(fn))` |
| `retrypolicy.NewBuilder[R]().WithMaxRetries(3).Build()` | `RetryPolicy(max_retries=3)` |
| `.HandleErrors(e)` / `.HandleErrorTypes(e)` | `handle=SomeError` |
| `.HandleResult(v)` | `handle=Result(v)` |
| `.HandleIf(pred)` | `handle=lambda result, exc: ...` |
| `.AbortOnErrors(e)` / `.AbortIf(pred)` | `abort=SomeError` / `abort=lambda r, e: ...` |
| `.WithDelay(d)` | `delay=1.0` |
| `.WithBackoff(d, max)` | `delay=Backoff(1.0, 30.0)` |
| `.WithJitterFactor(f)` | `delay=Backoff(1.0, 30.0, jitter=Jitter(factor=f))` |
| `.WithMaxAttempts(n)` | `max_retries=n - 1` (only `max_retries` is provided) |
| `.OnRetry(listener)` | `on_retry=listener` |
| `exec.Context()` | `current_execution()` |
| `circuitbreaker.NewWithDefaults[any]()` | `CircuitBreaker()` |
| `failsafehttp.NewRoundTripper(...)` | `failsafe.httpx.FailsafeTransport(...)` |
| `Policy[R]` / `ResultAgnosticPolicy[any]` | one `Policy` base — generics erasure removes the split |
| `ComposeAny`, `WithAny`, `ToExecutor(typeToken)` | not needed; `CircuitBreaker` composes with any result type |

## Behavioural differences

1. Sync `Timeout` is cooperative unless `interrupt=True`. With `interrupt=True` the caller is
   released on time, but the abandoned worker thread runs to completion.
2. **`HedgePolicy` is async-only.** Under `Failsafe` (sync) it raises `NotImplementedError`.
   Hedging needs cancellable concurrency; sync Python has none, and a thread-pool version would
   silently leak a thread per losing hedge.
3. **No externally-triggered cancellation in sync mode** — deadlines only. `exec.IsCanceled()` and
   `exec.Canceled()` have no sync equivalent. Async cancellation is normal `asyncio` cancellation.
4. No async-execution handles (`RunAsync` / `ExecutionResult`); use `asyncio.Task`, which already
   provides done/result/cancel.
5. Listener exceptions are logged, not propagated. A metrics bug will not break a request.
6. Cache keys derive from call arguments (`key=` callable) rather than a context value.
7. `CancelledError` and other `BaseException`s are never treated as failures and never retried.
8. `compose()` returns a new executor rather than mutating the receiver.
9. No `with` form for permit policies; use `acquire_permit()` / `release_permit()`.
10. **Listeners are lists, not single slots.** Go's `OnRetry(fn)` overwrites any previously
    registered listener; ours appends, and every kwarg accepts a callable or a sequence of them.
11. **`max_duration` is compared with `>=`, not Go's `>`.** Go only escapes a strict comparison
    because its wall clock drifts past the budget; with an injected clock the retry delay is
    clamped to land exactly on `max_duration`, and `>` would loop forever.
12. **A callable `delay=` yields no delay for a manual breaker transition.** Go keeps `DelayFunc`
    and `Delay` in separate fields, so `Open()` falls back to the fixed one. A single `delay=`
    union has nothing to fall back to.
13. **`AdaptiveThrottler.record_result(outcome)` replaces Go's `RecordResult` and `RecordError`.**
    Go needs two methods because a zero value is indistinguishable from "no result"; an
    `Outcome` carries both, matching `CircuitBreaker.record`.
14. **The throttler takes no `abort=`.** Go's throttler builder exposes no abort methods, so
    there is nothing to port.
15. **`on_success`/`on_failure` do not fire for a throttled rejection**, matching Go, where the
    hooks live in the executor that `PreExecute` short-circuits. A rejection is not an execution
    result, and it is not recorded in the window either.
16. **Policies with probabilistic behaviour accept `rng=`.** Go seeds the global `math/rand`;
    `RetryPolicy`, `CircuitBreaker` and `AdaptiveThrottler` each take an optional
    `random.Random`, so tests are exact rather than statistically likely.
17. **`RateLimiter.smooth(...)` / `RateLimiter.bursty(...)` replace Go's four constructors.**
    Go needs `NewSmooth`, `NewSmoothWithMaxRate`, `NewBursty` and a builder for each; two
    classmethods with keyword arguments cover all of it. `smooth()` takes either
    `max_executions` with a `period` or a `max_rate`, and rejects both or neither.
18. **`408 Request Timeout` is retryable; Go retries only 429 and 5xx-except-501.** A 408 is
    the server saying it gave up waiting for the request, which is exactly the transient
    condition a retry addresses. Go's omission looks like an oversight rather than a decision:
    its own `Retry-After` handling covers 429 and 503 only.
19. **A policy rejection surfaces as `httpx.TransportError`.** Go returns its own error from
    the `RoundTripper`, but httpx callers catch `httpx.HTTPError`, and a bare `FailsafeError`
    escaping a transport would bypass every `except httpx.HTTPError` in existing code. The
    original is chained as `__cause__`.
20. **`failsafe.httpx.retry_policy()` defaults `return_last_failure=True`.** Exhausting the
    retries on a failing *response* yields that response, not `RetriesExceeded`: a 500 is an
    answer the caller may want to inspect, and Go returns it too.
21. **`retry_after_delay` also honours the HTTP-date form of `Retry-After`.** Go parses only
    the integer-seconds form and ignores a date, which is equally legal per RFC 9110.
22. **No `failsafehttp.Request` equivalent.** Go offers both a `RoundTripper` and a `Request`
    wrapper because its `http.Client` can fail before the transport runs. httpx exposes the
    same hook for both, so one transport covers it.
23. **`retry_after_delay` returns `0.0` where Go's `DelayFunc` returns `-1`.** Go's sentinel
    means "fall back to the policy's own delay"; our single `delay=` union has no second
    field to fall back to, and a negative delay would be a negative sleep. A past or
    malformed `Retry-After` clamps to `0.0` for the same reason.
24. **Both transports accept `clock=`.** Go's transport reads the wall clock implicitly, so
    a `Retry-After` test must sleep for real. Injecting the clock keeps those tests exact
    and instant, and avoids tests reaching into private state to substitute one.
25. **The adaptive limiter computes exact quantiles, not a t-digest sketch.** Go feeds recent
    RTTs to `influxdata/tdigest`; we keep a sorted window. The limiter resets the structure on
    every limit update, so it is a batch quantile over ~50–500 samples, where a digest carries
    0.4–2.7% error and a sorted window carries none. Measured, not assumed.
26. **Priority and user are contextvars, not context values.** `priority_scope`,
    `level_scope` and `user_scope` replace Go's `ContextWithPriority` / `ContextWithLevel` /
    `ContextWithUser`. Same ambient-propagation semantics, no parameter threading.
27. **`Prioritizer.calibrate()` is explicit; the scheduler is an opt-in context manager.**
    Go's `ScheduleCalibrations` spawns a goroutine and hands back a `CancelFunc`. A library
    silently owning an OS thread is a worse bargain in Python, so `scheduled_calibrations(interval)`
    scopes the thread to a `with` block and joins it on exit.
28. **`Priority` is an `IntEnum`, and absence is `None` rather than `-1`.** Go returns `-1` from
    `FromContext` for "no priority"; `current_priority()` and `current_level()` return `None`,
    which cannot be mistaken for a valid level the way `-1` can be arithmetic'd into one.
29. **Prioritized policies are built by `prioritized_by(p)`, not a separate type.** Go has
    `PriorityLimiter` and `PriorityThrottler` interfaces produced by `BuildPrioritized`. One type
    per policy with an opt-in registration keeps the surface smaller and lets the same instance be
    described, metered and composed uniformly.
30. **`FifoSemaphore.release()` rechecks capacity before waking a waiter.** Go's `Release`
    decrements then wakes if `used < size`; our previous implementation handed the permit
    straight over, which was correct only at a fixed size. With the limiter resizing the
    semaphore downward, that would have pinned `used` above `size` indefinitely.

## One hazard worth stating plainly

Stateful policies model the health of a **backend**, not of a call site. Construct them once and
share them:

```python
# correct: one breaker for everything that talks to this backend
payments = Failsafe(retry, CircuitBreaker(), AdaptiveThrottler())

@payments
def charge(amount): ...
@payments
def refund(txn): ...
```

```python
# wrong: two independent breakers, each seeing half the traffic
@Failsafe(retry, CircuitBreaker())
def charge(amount): ...
@Failsafe(retry, CircuitBreaker())
def refund(txn): ...
```

Stateless policies (`RetryPolicy`, `Fallback`, `Timeout`) are safe to construct inline. Everything
stateful — `CircuitBreaker`, `AdaptiveThrottler`, `AdaptiveLimiter`, `Bulkhead`, `RateLimiter`,
`Budget` — must be hoisted. This mirrors failsafe-go, where policies are documented as safe to
share and intended to be shared; Python's decorator syntax just makes the wrong thing easier to
write by accident.

## Not ported

- gRPC integration (`failsafegrpc`).
- Server-side HTTP helpers (`NewHandler`, `NewHandlerWithLevel`) — deferred to roadmap phase 7.
  When they land they will keep Go's `X-Failsafe-Priority` / `X-Failsafe-Level` header names so
  Python and Go services interoperate.
