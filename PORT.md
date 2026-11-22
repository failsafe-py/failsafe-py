# Differences from failsafe-go

What this port does **differently from Go**, and why. For maintainers deciding whether
a Go behaviour applies here, and for anyone diffing the two libraries.

Not in scope: API spelling (builders became keyword arguments, `Get`/`Run` became
`call`), and anything a reader can see from a signature. Only decisions that change
what the library *does*.

## Capability gaps

Things Go can do that this port cannot, or does not.

1. **Hedging is async only.** `HedgePolicy` under `Failsafe` raises
   `NotImplementedError`. Cancelling a synchronous call is impossible in Python, so a
   thread-pool version would leave losing attempts running against the very dependency
   the policy protects — weaker semantics than Go under the same name. `AsyncFailsafe`
   cancels losers for real.

2. **No externally-triggered cancellation in sync mode.** Go exposes
   `exec.IsCanceled()` and a `Context` that any caller can cancel mid-execution. Sync
   Python has no preemption, so sync executions support *deadlines* only, checked
   between attempts. Async mode gets the full behaviour through task cancellation.

3. **No async execution handles.** Go's `RunAsync` returns an `ExecutionResult` because
   goroutines have no first-class handle. `asyncio.Task` already is one, so
   `asyncio.create_task(AsyncFailsafe(...).call(fn))` replaces the entire interface.

4. **No `failsafehttp.Request` equivalent.** Go offers both a `RoundTripper` and a
   request wrapper. httpx has one composition point, the transport, and two ways to do
   the same thing is worse than one.

5. **gRPC and server-side HTTP are not ported.** Go has interceptors and
   `NewHandler`; the ASGI equivalent is a different shape and is deferred.

## Semantic divergences

Same feature, different behaviour. These are the ones that will surprise someone who
knows Go.

6. **A hedge counts as an attempt but never as a retry.** Go's `CopyForHedge`
   increments `attempts` and `hedges`, leaving `retries` alone, so concurrent attempts
   never consume an outer retry's budget. Hedged attempts share their execution's
   counters but carry their own last result.

7. **An acceptable result always beats an expired hedge delay.** Go `select`s between
   the delay timer and the result channel, so when both are ready it picks arbitrarily
   and may start a hedge it no longer needs. This driver checks completed attempts
   first: deterministic, and strictly fewer duplicate requests.

8. **A budget-refused retry raises `BudgetExceeded`; a budget-refused hedge just stops
   hedging.** The attempts already in flight still produce a result. This mirrors Go,
   and is worth stating because one budget produces two different outcomes depending
   on which policy consulted it.

9. **`CancelledError` is never a failure.** Neither it nor any other `BaseException` is
   caught, handled or retried. Go has no equivalent concept, so nothing in its test
   corpus covers this.

10. **Listener exceptions are logged, not propagated.** Go lets a panic in a listener
    abort the execution. A metrics bug must not break a production request.

11. **One listener per event, fixed at construction.** Go's `OnRetry(fn)` overwrites
    any previously registered listener; this takes a single callable keyword and
    offers no post-construction registration. Combine observers into one function.

12. **Cache keys come from the call arguments.** Go reads a key out of the `Context`.
    Here `CachePolicy(cache, key=lambda user_id: ...)` is a function of the arguments,
    shaped like `functools.lru_cache`, because that is what a key almost always is.

13. **Priority and user are contextvars, not context values.** Ambient and
    request-scoped, as in Go, but propagated by `contextvars` rather than threaded
    through a parameter. Absence is `None`, not Go's `-1`.

14. **A bare value is a result condition.** `handle="bad"` and `handle=None` match a
    returned value; Go needs its separate `HandleResult` builder. Values that would
    read as something else — a tuple, a list, anything callable — need `Result(v)`, as
    does any member of a collection. Ambiguous and empty collections raise `TypeError`
    rather than being guessed at.

15. **The adaptive limiter computes exact quantiles, not a t-digest sketch.** Go feeds
    recent durations to `influxdata/tdigest` and resets it on every limit update,
    making it a batch quantile over ~50 samples. Measured against Go's own t-digest:
    2.66% error at n=50, 0.36% at n=500, where an exact sorted window is free of error
    at every size and simpler. The hedge policy's `MovingQuantile` *is* ported — it is
    windowless and serves a different purpose.

16. **The hedge quantile times each attempt, not the execution.** Matching Go, and
    worth recording because the obvious implementation is wrong: a hedged execution
    spans the stalled attempt *plus* the winner, so feeding that back inflates the
    estimate and suppresses future hedging — exactly when the tail is worst.

17. **`retry_after_delay` returns `0.0` where Go returns `-1`,** and clamps past or
    negative values to `0.0`. Go's sentinel means "no delay"; here a negative delay
    would become a negative sleep. The HTTP-date form of `Retry-After` is honoured,
    which Go does not parse.

18. **A policy rejection surfaces as `httpx.TransportError`.** Go returns its own error
    type from the `RoundTripper`. httpx callers catch `httpx.HTTPError`, and a bare
    `FailsafeError` escaping a transport would bypass every such handler. The original
    is chained as `__cause__`.

19. **Randomness is a seam, like time.** Every policy with probabilistic behaviour
    takes `rng=`. Go seeds the global `math/rand`, which makes its own tests
    loop-until-it-rejects; here they are exact.

## Known divergence in behaviour under load

20. **`FifoSemaphore.release()` rechecks capacity before waking a waiter.** Go's
    `Release` decrements and wakes if `used < size`. With the adaptive limiter
    shrinking the semaphore, handing the permit straight over pins `used` above `size`
    indefinitely. This is a bug fix, not a port difference, but Go's code reads
    differently and the reason is not obvious.

## One hazard worth stating plainly

Stateful policies — `CircuitBreaker`, `Bulkhead`, `RateLimiter`, `AdaptiveThrottler`,
`AdaptiveLimiter`, `Budget` — accumulate state across executions. What a shared
instance measures is whatever its callers have in common, so construct them once at
the scope you want to reason about:

```python
payments = Failsafe(retry, CircuitBreaker())   # one breaker, all payment calls

@payments
def charge(amount): ...
@payments
def refund(txn): ...
```

Building one per call site splits the signal:

```python
@Failsafe(retry, CircuitBreaker())             # a breaker seeing only charges
def charge(amount): ...
@Failsafe(retry, CircuitBreaker())             # another seeing only refunds
def refund(txn): ...
```

This mirrors failsafe-go, where policies are documented as safe to share and intended
to be shared. Python's decorator syntax just makes the wrong thing easier to write by
accident.
