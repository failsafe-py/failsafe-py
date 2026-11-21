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
- Server-side HTTP helpers (`NewHandler`, `NewHandlerWithLevel`) — deferred past v0.1. When they
  land they will keep Go's `X-Failsafe-Priority` / `X-Failsafe-Level` header names so Python and
  Go services interoperate.
