# Differences from failsafe-go

What this port does **differently from Go**, and why. For maintainers deciding whether
a Go behaviour applies here, and for anyone diffing the two libraries.

Not in scope: API spelling (builders became keyword arguments, `Get`/`Run` became
`call`), and anything a reader can see from a signature. Only decisions that change
what the library *does*.

## Capability gaps

Things Go can do that this port cannot, or does not.

1. **gRPC and server-side HTTP are not ported.** Go has interceptors and
   `NewHandler`; the ASGI equivalent is a different shape and is deferred (never?).

1. **Hedging is async only.** `HedgePolicy` under `Failsafe` raises
   `NotImplementedError`. Cancelling a synchronous call is impossible in Python, so a
   thread-pool version would leave losing attempts running against the very dependency
   the policy protects — weaker semantics than Go under the same name. `AsyncFailsafe`
   cancels losers for real.

1. **No externally-triggered cancellation in sync mode.** Go exposes
   `exec.IsCanceled()` and a `Context` that any caller can cancel
   mid-execution. Sync Python has no obvious way to do this. Async mode gets
   the full behaviour through task cancellation.

1. **No `failsafehttp.Request` equivalent.** Go offers both a `RoundTripper` and a
   request wrapper. httpx has a single composition point so Python is simpler here.

## Semantic divergences

Same feature, slightly different behaviours.

Note: this is mostly an agent being a bit too specific about differences, read
at your own risk.

1. **A hedge counts as an attempt but never as a retry.** Go's `CopyForHedge`
   increments `attempts` and `hedges`, leaving `retries` alone, so concurrent attempts
   never consume an outer retry's budget. Hedged attempts share their execution's
   counters but carry their own last result.

1. **An acceptable result always beats an expired hedge delay.** Go `select`s between
   the delay timer and the result channel, so when both are ready it picks arbitrarily
   and may start a hedge it no longer needs. This driver checks completed attempts
   first: deterministic, and strictly fewer duplicate requests.

1. **A budget-refused retry raises `BudgetExceeded`; a budget-refused hedge just stops
   hedging.** The attempts already in flight still produce a result. This mirrors Go,
   and is worth stating because one budget produces two different outcomes depending
   on which policy consulted it.

1. **`CancelledError` is never a failure.** Neither it nor any other `BaseException` is
   caught, handled or retried. Go has no equivalent concept, so nothing in its test
   corpus covers this.

1. **Listener exceptions are logged, not propagated.** Go lets a panic in a listener
    abort the execution. A metrics bug must not break a production request.

1. **One listener per event, fixed at construction.** Go's `OnRetry(fn)` overwrites
    any previously registered listener; this takes a single callable keyword and
    offers no post-construction registration. Combine observers into one function.

1. **Cache keys default to the call arguments.** Go only reads a key out of the
    `Context`. Here `CachePolicy(cache, key=lambda user_id: ...)` is a function of the
    arguments, shaped like `functools.lru_cache`, because that is what a key almost
    always is. Go's mechanism exists too — a `cache_key(...)` block overrides `key=`,
    just as a context key overrides the policy's in Go — so this adds a spelling
    rather than replacing one. With neither, the execution runs uncached.

1. **Priority and user are contextvars, not context values.** Ambient and
    request-scoped, as in Go, but propagated by `contextvars` rather than threaded
    through a parameter. Absence is `None`, not Go's `-1`.

1. **A bare value is a result condition.** `handle="bad"` and `handle=None` match a
    returned value; Go needs its separate `HandleResult` builder. Values that would
    read as something else — a tuple, a list, anything callable — need `Result(v)`, as
    does any member of a collection. Ambiguous and empty collections raise `TypeError`
    rather than being guessed at.

1. **The adaptive limiter computes exact quantiles, not a t-digest sketch.** Go feeds
    recent durations to `influxdata/tdigest` and resets it on every limit update,
    making it a batch quantile over ~50 samples. Measured against Go's own t-digest:
    2.66% error at n=50, 0.36% at n=500, where an exact sorted window is free of error
    at every size and simpler. The hedge policy's `MovingQuantile` *is* ported — it is
    windowless and serves a different purpose.

1. **The hedge quantile times each attempt, not the execution.** Matching Go, and
    worth recording because the obvious implementation is wrong: a hedged execution
    spans the stalled attempt *plus* the winner, so feeding that back inflates the
    estimate and suppresses future hedging — exactly when the tail is worst.

1. **A policy rejection surfaces as `httpx.TransportError`.** Go returns its own error
    type from the `RoundTripper`. httpx callers catch `httpx.HTTPError`, and a bare
    `FailsafeError` escaping a transport would bypass every such handler. The original
    is chained as `__cause__`.


