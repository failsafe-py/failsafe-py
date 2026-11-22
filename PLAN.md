# failsafe-py — Design & Plan

A Python port of [failsafe-go](https://github.com/failsafe-go/failsafe-go): the same policies,
composition model, failure-handling semantics and event model, expressed in Python idiom rather
than transliterated from Go.

This document is working material. It is deleted once the library ships.

---

## 1. Goal and non-goals

**Non-goals:**

- gRPC integration (`failsafegrpc`).
- Go's `RunAsync` / `ExecutionResult` handle — Python has `asyncio.Task` (D-9).
- Anything failsafe-go does not have: no control plane, no framework bootstrap, no codegen, no
  feature toggles, no per-policy observability integrations. Scope is failsafe-go's, no wider.

**Invariants:**

- `BaseException` is never swallowed. `KeyboardInterrupt`, `SystemExit` and especially
  `asyncio.CancelledError` propagate untouched — a cancelled task is not a retryable failure.
  The core catches `Exception`, never `BaseException`. This is the most common correctness bug in
  Python resilience libraries.
- Policy objects are shareable across threads, tasks and executions, as in Go.
- Stdlib only in the core.

---

## 2. Dependencies

Zero runtime dependencies for the core. Optional extras at integration boundaries only
(`failsafe-py[httpx]`). Dev-time: pytest, mypy, ruff, uv.

failsafe-go is itself not dependency-free, and both of its deps have stdlib equivalents:

| Go dep | Used for | Python replacement |
|---|---|---|
| `bits-and-blooms/bitset` | ring buffer of success/failure bits in `countingStats` | `collections.deque`, or an `int` as a bit vector |
| `influxdata/tdigest` | streaming quantiles in the adaptive limiter | a bounded sorted window (`_windows.QuantileWindow`) |

Caveat: stdlib-only is free for every policy *except* the adaptive limiter, which needs a
different internal data structure rather than a different library. Go samples recent RTTs into a
t-digest; the replacement is a sorted window.

**Corrected at phase 5.** Earlier drafts of this section offered `util.MovingQuantile` as the
replacement. That is the wrong structure: `MovingQuantile` is a *windowless* estimator used by
`hedgepolicy`, and nothing in `adaptivelimiter` touches it. The limiter resets its
t-digest on every limit update, so it is a *batch* quantile over one window's samples —
`recentWindowMinSamples` defaults to 50.

**Confirmed at phase 6.** `MovingQuantile` was ported verbatim for `HedgePolicy`'s adaptive
delay, and the limiter still uses the sorted window. The two estimators coexist because they
answer different questions: the limiter wants an exact quantile over a bounded batch, the hedge
policy wants an O(1) estimate over an unbounded stream.

The accuracy claim was then measured rather than asserted, running Go's own `influxdata/tdigest`
against an exact p90 over lognormal samples, 200 trials per size:

| Window samples | t-digest mean relative error on p90 | Sorted window |
|---:|---:|---:|
| 50 | 2.66% | exact |
| 100 | 1.38% | exact |
| 200 | 0.68% | exact |
| 500 | 0.36% | exact |

So the sorted window is both simpler and strictly more accurate at every size this policy reaches;
t-digest earns its keep in the millions, not the tens. `bisect.insort` is O(n) per sample against
the digest's O(log n), which at n ≤ a few hundred is memmove on a contiguous list and not worth a
dependency. `tests/test_windows.py::test_quantile_window_beats_a_sketch_at_limiter_sample_counts`
pins the exactness.

Rejected dependencies:

| Library | Would give | Why not |
|---|---|---|
| `anyio` | one async API over asyncio+trio; cancel scopes; task groups | hard runtime dep on every user; trio support is not a goal. The sans-io design needs ~150 lines of what it offers. Reconsider only if trio is requested |
| `tenacity` | battle-tested retry | solves 1 of 12 policies, and brings an API model that fights ours |
| `freezegun` | time travel in tests | global patching, slow, surprising. An injected clock is better and is required by the design anyway (§9) |
| `wrapt` | bulletproof decorators | `functools.wraps` suffices |

---

## 3. Feature parity matrix

| failsafe-go package | LOC | Python name | Phase |
|---|---:|---|---|
| `failsafe` (Executor/Execution) | 973 | `Failsafe`, `AsyncFailsafe`, `Execution` | 1 |
| `policy`, `common`, `internal` | 629 | `failsafe._core` | 1 |
| `retrypolicy` | 636 | `RetryPolicy` | 1 |
| `fallback` | 194 | `Fallback` | 1 |
| `timeout` | 143 | `Timeout` | 1 |
| `circuitbreaker` | 1591 | `CircuitBreaker` | 1 |
| `adaptivethrottler` | 903 | `AdaptiveThrottler` | 2 |
| `ratelimiter` | 838 | `RateLimiter` | 3 |
| `bulkhead` | 295 | `Bulkhead` | 3 |
| `failsafehttp` | 1223 | `failsafe.httpx` | 3 |
| `adaptivelimiter` | 2012 | `AdaptiveLimiter` | 5 |
| `priority` | 1233 | `failsafe.priority` | 5 |
| `hedgepolicy` | 369 | `HedgePolicy` | 6 |
| `cachepolicy` | 203 | `CachePolicy` | 6 |
| `budget` | 154 | `Budget` | 6 |
| `failsafegrpc` | 903 | — | out of scope |

---

## 4. Go construct → Python

| Go construct | Role | Python counterpart | Friction |
|---|---|---|---|
| `(R, error)` returns | every result is a pair; policies inspect both | exceptions | **High.** Needs an internal `Outcome`; raise only at the boundary (D-2) |
| `errors.Is` | `HandleErrors(ErrConnecting)` | `isinstance` + `__cause__` walk | Medium. Go sentinels ≈ Python exception *types* (D-3) |
| `errors.As` | type match through `Unwrap` | `isinstance` | collapses into the above: 2 Go methods → 1 |
| generics `[R any]` | typed results and listeners | `Generic[R]`, erased at runtime | **Low — a simplification.** `ComposeAny`, `WithAny`, `ResultAgnosticPolicy`, `ToExecutor(typeToken)` all disappear |
| `context.Context` | cancellation + deadline + value bag + plumbing | four separate mechanisms | **High** (D-5) |
| goroutine | async exec, hedging, timeout race | `asyncio.Task` / `threading.Thread` | **High.** No preemption in sync mode (D-7, D-8) |
| `select { case <-ctx.Done() }` | cooperative cancellation | `asyncio.wait` | Medium |
| `time.Duration` | all delays | `float` seconds | Low (D-6) |
| builder + `Build()` | policy config | keyword-only constructor | Low (D-1) |
| `reflect.DeepEqual` | `HandleResult` matching | `==` | Low |
| `sync.Mutex`, `atomic.*` | shared policy state | `threading.Lock` | Low |
| `testing/synctest` | virtual clock in tests | injected clock (§9) | Medium |
| `http.RoundTripper` | client middleware | `httpx.BaseTransport` | **Low — near-exact analogue** (D-12) |
| `http.Handler` middleware | server side | ASGI middleware | deferred — roadmap phase 7 |

---

## 5. Decisions

### D-7 — Sans-io core, two thin drivers

Policy logic is a generator yielding *instructions*; a driver decides how to service them:

```python
def apply(self, inner):
    def step(ex):
        while True:
            outcome = yield from inner(ex)        # nested policies compose via yield from
            if not self.is_failure(outcome) or exceeded: return outcome
            yield Sleep(self.compute_delay(ex))   # driver: time.sleep | await asyncio.sleep
    return step
```

Alternatives rejected: async-only (excludes sync users — a large share of the ecosystem);
sync-only (non-starter); two full implementations (every policy, fix and test written twice,
divergence guaranteed); unasync codegen (build step, generated files in-tree, and it breaks down
exactly where the modes differ most).

The decisive property: **one policy instance is shared between sync and async callers.** A
`CircuitBreaker` tripped by a sync call is seen open by an async call — prototyped and asserted.
Neither duplicate implementations nor codegen can offer this, and it matters wherever a sync
worker and an async service guard the same backend.

Policies needing **zero** per-mode code: retry, fallback, circuit breaker, cache, rate limiter,
budget, adaptive throttler. Per-mode code is confined to the drivers (`Invoke`, `Sleep`,
`Acquire`, `Race`, `Hedge`), roughly 150 lines each.

**Cost.** Measured against a localhost HTTP GET (`urllib`, loopback, N=2000 — the fastest real I/O
anyone will wrap):

| localhost HTTP GET | µs/call | vs bare |
|---|---:|---:|
| bare `urlopen` | ~170 | — |
| + 1 policy | ~175 | +2–3% |
| + 4 policies | ~180 | +5–6% |

~2–3% for a single-policy composition on the fastest possible real call, trending to zero as the
call slows: 0.15% at 1 ms, 0.01% at 10 ms. The core's own cost is ~1.5 µs/call and does not scale
with what the wrapped function does.

That profiling produced one binding requirement: generator creation is only ~50 ns, but per-call
setup is not. Composing the policy chain once at construction and giving `Execution` `__slots__`
takes a naive implementation from ~1485 to ~745 ns/call. **`_core` must do both** — a requirement,
not a later optimisation.

**Sync `Timeout`:** cooperative by default (matching Go's behaviour for code that ignores its
`ctx`), with `Timeout(..., interrupt=True)` opting into a worker thread. Interrupt mode frees the
caller on time but the abandoned thread runs to completion — a real leak, documented, with a
`ponytail:` comment naming the ceiling.

### D-1 — Keyword constructors, not builders

Java needed builders (no kwargs); Go copied Java; Python needs neither. Union-typed parameters
collapse the clusters Go spreads across many setters.

Counted from the Go builder interfaces, including inherited methods:

| Policy | Go builder methods | After grouping | Non-event kwargs |
|---|---:|---:|---:|
| RetryPolicy | 26 | 14 | **8** |
| CircuitBreaker | 23 | 11 | **5** |
| AdaptiveLimiter | 14 | 11 | **9** |
| AdaptiveThrottler | 8 | 6 | **4** |
| HedgePolicy | 7 | 5 | **7** |
| Fallback | 7 | 5 | **3** |
| CachePolicy | 5 | 5 | **3** |
| Bulkhead | 4 | 4 | **1** |
| RateLimiter | 2 | 2 | **1** |
| Timeout | 1 | 1 | **1** |

Worst case is RetryPolicy at 8 substantive parameters plus 6 event hooks. The collapses:

| One parameter | Replaces |
|---|---|
| `delay=` | `WithDelay`, `WithDelayFunc`, `WithBackoff`, `WithBackoffFactor`, `WithRandomDelay`, `WithJitter`, `WithJitterFactor` |
| `handle=` | `HandleErrors`, `HandleErrorTypes`, `HandleResult`, `HandleIf` |
| `abort=` | `AbortOnErrors`, `AbortOnErrorTypes`, `AbortOnResult`, `AbortIf` |
| `max_retries=` | `WithMaxRetries`, `WithMaxAttempts` — `max_retries` is canonical (matches `requests`/`urllib3`); Go's `WithMaxAttempts` spelling is not ported |

**Jitter belongs to the delay.** Go's `util.ApplyJitter(delay, jitter, jitterFactor)` is applied
*to* a computed delay, and Go's own docs note jitter is ignored when no delay is configured — so a
top-level `jitter=` is a parameter that silently does nothing in a valid-looking config. It
becomes an attribute of the delay strategy instead:

```python
delay=Backoff(1.0, 30.0, jitter=Jitter(factor=0.25))   # ±25% of each delay
delay=Backoff(1.0, 30.0, jitter=Jitter(seconds=0.1))   # ±100 ms on each delay
delay=1.0                                              # no jitter, nothing to misconfigure
```

**`handle` and `abort` are the same kind of thing** — both are "does this result-or-error match?"
filters. Go proves it: `BaseFailurePolicy.failureConditions` and
`BaseAbortablePolicy.abortConditions` are both `[]func(R, error) bool` evaluated by the same
`util.AppliesToAny`. They differ only in what the policy does on a match, so they take the same
argument type (D-3).

`AdaptiveLimiter` at 9 tuning parameters was the one policy where a nested config object might
have been warranted. Settled at phase 5, after building it: it stays flat. See D-17.

### D-2 — Results vs exceptions

A private `Outcome` (`value` xor `exc`) threaded through every policy layer and unwrapped at the
boundary — the analogue of `common.PolicyResult[R]`, and what makes value-matching conditions and
`Fallback` possible at all.

Policy errors keep the last failure *and* chain the traceback:

```python
raise RetriesExceeded(last_result=..., last_exception=exc) from exc
```

### D-3 — One `Condition` type

Go needs four `Handle*` methods because `errors.Is` ≠ `errors.As` and generics split results from
errors. Python has `isinstance`:

```python
Condition = (
    type[Exception]                                  # exception type (errors.Is + errors.As)
    | tuple[type[Exception], ...] | list[...]        # OR'ed exception types
    | Callable[[R | None, Exception | None], bool]   # arbitrary predicate (HandleIf)
    | Any                                            # any other value (HandleResult)
    | Result(value)                                  # a value that reads as one of the above
)
```

Bare values are the common spelling: `handle="bad"`, `handle=None`, `handle=429`. The default
is a private `UNSET` sentinel rather than `None`, since `handle=None` has to mean "a `None`
return is a failure" and would otherwise be indistinguishable from omitting the argument.

`Result(...)` survives for the three shapes a bare value cannot take, because each already
means something: a tuple or list (exception types, OR'ed conditions) and anything callable
(a predicate). Inside a collection every member must be an exception type or a `Result`, since
a collection already means "any of these"; a bare value there raises `TypeError`, as does an
empty collection.

The same type is reused wherever Go has a condition family: `abort=` on retry, `cancel_on=` on
hedge, `cache_if=` on cache. Four builder families, one concept.

Go's OR-semantics and the rule that *a result-only condition does not replace the default error
handling* must be preserved verbatim; both are load-bearing and easy to get wrong.

Python-specific rules:

- **Chain walking:** `errors.Is` follows `Unwrap`; `isinstance` does not follow `__cause__`. Walk
  `__cause__` only (explicit chaining), depth-limited. Walking `__context__` would match
  exceptions raised incidentally during handling.
- **`ExceptionGroup`:** match if any leaf matches.

### D-4 — Entry point

Go has 8 execute methods (`Run`/`Get` × `WithExecution` × `Async`). Python collapses `Run`/`Get`
(no void/value split) and drops `Async` (D-9), leaving one: a `Failsafe` object that is callable
and usable as a decorator.

Rejected: a module function (no reusable composition object); stacked decorators (no shared
`Execution`, no composition-level events, ordering implicit in decorator order); `with failsafe(...)`
(unsound — a `with` body cannot be re-entered, so retry is impossible). No `with` form exists for
permit policies either; one shape, no per-policy exceptions. Standalone permit acquisition is
plain methods (D-14), trivially wrapped in `contextlib.contextmanager` by anyone who wants it.

**One policy set per backend, reused across call sites.** For stateful policies this is mandatory:
an `AdaptiveThrottler` or `CircuitBreaker` models the health of a *backend*, not of a call type,
so all calls to that backend must share one instance or the signal fragments.

```python
payments = Failsafe(retry, payments_breaker, payments_throttler)   # module or DI scope

def charge(amount):  return payments.call(_post, "/charge", amount)
def refund(txn):     return payments.call(_post, "/refund", txn)

@payments                                   # the same object, as a decorator
def status(txn): ...
```

**The fragmentation hazard.** `@Failsafe(RetryPolicy(), CircuitBreaker())` written on twenty
functions constructs twenty independent breakers — measured, and a silent correctness bug for
every stateful policy. No type checker can reject it: it is a valid constructor call. Mitigations
considered:

| Option | Sharing | Cost |
|---|---|---|
| **Shared object** *(chosen)* | correct if the user hoists | module-level objects; relies on discipline |
| Decorator accepts only a `Failsafe` | correct by construction | blocks the legitimately-stateless one-liner |
| Named registry (`Failsafe.for_backend("payments", …)`) | correct by construction | a global registry: testing, teardown, name collisions |
| Lazy factory (`Failsafe(make)`) | **no** — measured; two decorators given the same factory still build two policy sets, because sharing is about *how many* get built, not *when* | dropped |

Chosen: the shared object, plus documentation and optionally a lint rule. It is what failsafe-go
users already write (`var breaker = circuitbreaker.New...`), has no hidden global, and is the
easiest thing to test. The named registry stays on the shelf — adding it later breaks nothing.

Consequences: docs lead with the backend-scoped object, never an inline decorator on a stateful
policy; the hazard is documented in `PORT.md`. `fs.compose(inner)` returns a **new** `Failsafe`
(Go mutates the receiver — not copied).

### D-5 — Cancellation and ambient state

| Job | Python answer |
|---|---|
| cancellation (async) | task cancellation → `CancelledError`, propagated untouched |
| cancellation (sync) | **not supported** |
| deadline (async) | `asyncio.timeout` — an enclosing scope that interrupts the attempt and exposes `.when()` |
| deadline (sync) | tracked by the driver between attempts; passed down as an I/O timeout |
| ambient values (priority, cache key) | contextvars or explicit params (D-11) |
| downstream plumbing | async: task cancellation reaches httpx for free; sync: map remaining budget to `timeout=` |

**No externally-triggered cancellation in sync mode.** Sync code sets a deadline; it does not get
cancelled from another thread. No mainstream sync library exposes such a channel — `requests`,
`urllib` and DB-API drivers all take a timeout, none take a cancel token — so a `threading.Event`
on `Execution` would be API surface nothing cooperates with. Sync supports **deadlines only**
(`Timeout`, `max_duration`), plus `Timeout(interrupt=True)` as the thread-based escape hatch.
Async keeps real cancellation.

Consequently `Execution` has no `cancelled` attribute; it exposes `deadline` / `remaining()`,
which the driver enforces between attempts and user code can read to size its own I/O timeouts.
This removes a Go concept (`exec.IsCanceled()`, `exec.Canceled()`) rather than faking it.

`asyncio.timeout` is the async deadline mechanism, not something to reimplement: a context manager
that interrupts whatever awaits inside it and exposes the absolute deadline via `.when()`.
`asyncio.wait_for` is the older single-coroutine equivalent and is less suited, because a policy
composition wraps a whole scope rather than one awaitable.

**User code reaches the `Execution` through a contextvar**, `failsafe.current_execution()`. This is
a place where Python is better than Go: contextvars give correctly-scoped ambient access without
threading a parameter through every signature, and `copy_context()` carries it into hedge threads
and tasks. The explicit Go-style alternative (`call_with_execution(fn)` → `fn(exec)`) would double
every execute method and remain unusable from nested helpers.

### D-6 — Durations

`float` seconds, also accepting `timedelta`, normalized by one boundary helper. Matches
`time.sleep`, `asyncio.timeout`, httpx.

A `Delay` is any of:

| Form | Means | Go equivalent |
|---|---|---|
| `1.0` / `timedelta(seconds=1)` | fixed delay | `WithDelay` |
| `Fixed(1.0, jitter=…)` | fixed delay, explicit form when jitter is wanted | `WithDelay` + `WithJitter*` |
| `Backoff(1.0, 30.0, factor=2.0, jitter=…)` | exponential to a max | `WithBackoff`, `WithBackoffFactor` |
| `RandomDelay(1.0, 3.0)` | uniform in a range | `WithRandomDelay` |
| `lambda exec: …` | computed per attempt | `WithDelayFunc` |

`Jitter(factor=…)` or `Jitter(seconds=…)` attaches to any non-callable form; the wrapper keeps
Go's two distinct meanings without a polymorphic float where "<1 means factor" would silently
misread `jitter=0.5`. Bare `jitter=0.25` is accepted as shorthand for `Jitter(factor=0.25)`, but
the wrapper is canonical. A plain float carries no jitter — nothing to leave half-configured.

### D-8 — Hedging is async-only

`HedgePolicy` is fully supported under `AsyncFailsafe`, where cancellation of losing attempts is
real. Under `Failsafe` (sync) it raises `NotImplementedError` pointing at `AsyncFailsafe`.

An internal event loop does not rescue sync mode. Measured: a sync function cannot be awaited, so
running it under an internal loop means `asyncio.to_thread` — the threads are still there, plus a
loop. Worse, with a 0.4 s original and a 0.01 s hedge, the hedge resolves in 13 ms but
`asyncio.run()` then blocks **393 ms** at shutdown joining the abandoned thread. A plain
`ThreadPoolExecutor` returns in 13 ms. The internal loop converts an invisible background leak
into caller-visible latency.

A thread-pool version would ship weaker-than-Go semantics (uncancellable losers) under the same
policy name. This is the one capability gap versus failsafe-go.

**Held at phase 6.** Implemented as written: the sync driver raises `NotImplementedError` on the
`Hedge` instruction, and `tests/test_hedge.py` pins that losing attempts observe a real
`CancelledError`. Concurrency lives entirely in the async driver, behind a `HedgeControl`
protocol that keeps delays, cancellation conditions and the budget in the policy.

### D-9 — No async execution handle

`RunAsync` / `ExecutionResult` are dropped. Go needs them because goroutines have no first-class
handle; `asyncio.create_task(afs.call(fn))` already provides done/result/cancel with the stdlib's
own semantics. This deletes an entire interface and 4 of Go's 8 execute methods.

### D-10 — Event listeners

A single optional listener per event, passed as a constructor keyword. There is no
post-construction registration: a policy's observers are visible where it is built.

Two deviations from Go:

- **Sync listeners only.** Awaiting a coroutine listener is impossible in sync mode; allowing them
  would leak function colouring into the event model.
- **Listener exceptions are logged, not propagated.** Go lets panics escape; a metrics bug must not
  break a production request.

Events are frozen dataclasses mirroring Go's: `ExecutionEvent`, `ExecutionScheduledEvent`,
`ExecutionDoneEvent`, `StateChangedEvent`, `LimitChangedEvent`.

### D-11 — Cache keys and priority

Go stashes both in the `Context` because that is its only ambient channel. Split them by how they
are used:

- **Cache key: a `key=` callable over the call arguments** — `CachePolicy(cache, key=lambda url: url)`,
  shaped like `functools.lru_cache`. The key is a function of the arguments in nearly all cases.
- **Priority/level: a contextvar**, Go-style. Priority genuinely is request-scoped ambient data
  propagated across HTTP hops, and the header propagation in D-12 needs it there anyway.

### D-12 — HTTP via httpx

`http.RoundTripper` ↔ `httpx.BaseTransport` is a near-exact match, and httpx has both sync and
async transports — the same dual shape as the core. `requests` is sync-only with no cancellation
story; supporting three clients would be 3× maintenance for one feature.

```python
client = httpx.Client(transport=failsafe.httpx.FailsafeTransport(retry, breaker))
retry = failsafe.httpx.retry_policy(max_retries=3, delay=Backoff(1, 30))
```

Non-obvious behaviours to port from `failsafehttp`:

- default retryable set: connection errors, 408/429/5xx, honouring `Retry-After`
  (`failsafe.httpx.retry_after_delay`, usable in a `CircuitBreaker` too, as Go's `DelayFunc` is);
- **request body rewind between attempts** (Go's `bodyReader`) — without it a retry silently sends
  an empty body. A correctness item, not an optimisation;
- streaming responses: the execution is not done until the stream closes (Go wraps `resp.Body` to
  defer cancellation).

Client side only. Go's server-side helpers (`NewHandler`, `NewHandlerWithLevel`, priority
header decoding) are deferred; when they land they keep Go's header names `X-Failsafe-Priority`
and `X-Failsafe-Level` so Python and Go services interoperate.

### D-13 — Layout, naming, Python version

Go needs one package per policy (`retrypolicy.Builder`) for namespacing. Python does not:

```
failsafe/
  __init__.py        # Failsafe, AsyncFailsafe, policies, exceptions, current_execution
  _core.py           # Outcome, instructions, Execution
  _sync.py _async.py # the drivers
  retry.py  circuit_breaker.py  timeout.py  fallback.py  bulkhead.py
  rate_limiter.py  hedge.py  cache.py  budget.py
  adaptive/          # limiter + throttler + priority
  httpx.py           # optional extra
```

**PyPI:** `failsafe` is taken (an unrelated squatted 0.1.0), `pyfailsafe` is taken (dormant since
2017). Distribute as `failsafe-py`, import as `failsafe`.

**Python 3.11+**: `asyncio.timeout`, `TaskGroup`, `ExceptionGroup`, `Self`.

### D-14 — Standalone policy APIs

Blocking standalone methods (rate limiter, bulkhead, adaptive limiter) need both colours:
`acquire_permit()` and `acquire_permit_async()` **on the same object**. Non-blocking ones
(`try_acquire_permit`, `record_success`, breaker state) need only one.

Separate `AsyncBulkhead` classes (httpx-style) are rejected because that splits the *state*. One
`FifoSemaphore` instance can serve both: two sync threads holding permits while an asyncio task
blocks on the same semaphore until they release, with FIFO fairness across both worlds and no
leaked waiters on timeout — prototyped.

### D-15 — Typing

- **`Failsafe` is not `Generic[R]`; `call` is.** Inferring `R` per call keeps
  `fs.call(connect, host)` typed with no annotation, and lets one composition guard
  calls returning different types.

  An earlier draft justified this by the Python floor: that PEP 696 defaults need 3.13,
  and that `Failsafe(retry)` would otherwise infer `Never`. Both are wrong.
  `typing_extensions.TypeVar` backports PEP 696 to 3.11, and a class variable with a
  default infers `Failsafe[Any]`, not `Never`; only an undefaulted one gives `Never`.

  The real reason is that a composition-level `R` conflicts with reusing a composition.
  Measured: with `Fallback` generic in its value, `Failsafe(Fallback(value=cached),
  RetryPolicy())` binds `R` to the fallback's type, after which any call returning
  something else is a type error — and `Fallback(value=None)` pins the whole
  composition to `None`. That breaks the one-per-backend pattern this library
  recommends, to catch a wrong-typed fallback that the first test run catches anyway.
- **`call` takes a `ParamSpec`**, so arguments are checked against the wrapped
  function's signature, not just its return type. `tests/test_typing.py` pins this:
  the deliberate errors there carry `type: ignore` comments, and strict mode's
  unused-ignore warning fails the build if a check stops working.
- Go's `ResultAgnosticPolicy`, `ComposeAny`, `WithAny` and type tokens all vanish —
  `CircuitBreaker` composes with any result type for free under mypy.
- `py.typed`; strict mypy and pyright clean. Bad typing is the top complaint about Python
  resilience libraries.
- `ParamSpec` on the decorator so signatures survive.

### D-16 — API reference

pkg.go.dev is automatic, canonical, zero-config and registry-side: publish a module and the
reference exists. **Python has no equivalent** — PyPI renders only the README, and nothing
introspects a published wheel. failsafe-go.dev is hand-written prose with *no* API listing because
pkg.go.dev covers that for free, so porting only the prose would drop half of what a failsafe-go
user relies on. The reference is parity, not polish.

Constraints: the Jekyll prose site is not to be rewritten; hosting should require no work; two
sites are acceptable, and per-version API docs argue for it.

**Chosen: Jekyll prose on GitHub Pages, API reference on Read the Docs.**

- **Zero hosting work** — RTD builds on push and serves the result; no CI step of ours can rot.
- **Per-version docs free** — RTD builds each tag and branch: `/en/v0.1/`, `/en/stable/`, and a
  version dropdown. Hand-rolling this under GitHub Pages is manual per deploy, and it matters for
  a library whose users pin versions.
- **Free custom domain with HTTPS.**
- **MkDocs natively** (RTD builds MkDocs as well as Sphinx), so the reference uses
  `mkdocs-material` + `mkdocstrings`, reading the type hints committed to under D-15. The Jekyll
  site is untouched — no Ruby work, ever.

**Never the stock RTD/Sphinx theme** — it is dated. With MkDocs the theme is ours;
`mkdocs-material` by default, with a palette tuned to the failsafe-py logo. If the reference ever
moves to Sphinx, use Furo or shibuya rather than `sphinx_rtd_theme`.

Fallback, on record if the two-site split ever grates: pdoc HTML committed under `/api/` in the
Jekyll site. Jekyll copies front-matter-less files verbatim without Liquid processing
([Static Files](https://jekyllrb.com/docs/static-files/)), the existing site already relies on
this for `assets/js/*`, and pdoc output contains zero Liquid-hostile sequences (verified). That
path was never executed end-to-end — if chosen, confirm one rendered `/api/` page on first deploy.

Split search between prose and reference is not a regression: failsafe-go.dev and pkg.go.dev
already search separately.

Implications:

- **Docstrings are definition-of-done** for every public symbol, from the first commit. Google
  style. They are the whole input to the reference.
- Port failsafe-go's doc comments where they apply — they are good, and they are the spec.
- **The strict gate, verified rather than assumed.** `fail_on_warning: true` is a real MkDocs
  key on Read the Docs and does mean `--strict`, but it applies only to RTD's *default* build
  step. `.readthedocs.yaml` overrides that step to run under `uv`, so the gate lives in the
  command — `mkdocs build --strict` — and `fail_on_warning` would be inert. Both were measured:
  renaming a parameter without updating its docstring fails the build with
  *"Parameter 'max_retries' does not appear in the function signature"*, exit 1; a reference to
  a symbol that does not exist fails with *"could not be found"*. That is the pkg.go.dev
  always-current property, and it is why the gate is never relaxed to make a build pass.
- **Read the Docs installs with `uv` natively**, via `python.install` with `method: uv`,
  `command: sync`, `groups: [docs]`. No `pip install uv` bootstrap, and the lockfile is honoured.


### D-17 — `AdaptiveLimiter` stays flat (settles the D-1 deferral)

D-1 deferred this until the policy had been used. Having now built and tested it: **flat
keyword-only parameters, no nested config object.**

The count that motivated the deferral was 9 "tuning parameters", but they do not form 9
independent axes a caller tunes. Two unions collapse what Go spreads across separate setters:

| One parameter | Replaces Go's |
|---|---|
| `queueing=(initial, max)` | `WithQueueing(initialRejectionFactor, maxRejectionFactor)` |
| `prioritized_by(prioritizer)` | `BuildPrioritized(p)` |

What remains is one default-everything constructor (`AdaptiveLimiter()` matches Go's
`NewWithDefaults`) plus parameters that are individually meaningful and essentially never set
together. A nested `LimiterConfig` would add a second import, a second name to document, and an
extra indirection on every attribute read, in exchange for grouping parameters that callers set
one at a time. The tests bear this out: no test in `tests/test_adaptive_limiter.py` sets more
than three non-default parameters, and the common cases set zero or one.

This stays additive either way — a config object can be accepted later without breaking the
keyword form, which is precisely why deferring it cost nothing.

### D-18 — Calibration is explicit, not a library-owned thread

Go's `ScheduleCalibrations(ctx, interval)` spawns a goroutine and returns a `CancelFunc`.
Goroutines are cheap and ubiquitous in Go; a library silently owning an OS thread in Python is
not the same bargain — it interacts badly with `fork`, with test suites, and with anyone running
under an event loop who never asked for a thread.

So `Prioritizer.calibrate()` is a plain method the caller schedules, and
`Prioritizer.scheduled_calibrations(interval)` is an opt-in **context manager** that owns a daemon
thread only for the duration of the block and joins it on exit. The context manager makes the
lifetime lexical, which the Go `CancelFunc` leaves to the caller's discipline.

No async variant is provided. The calibration body is pure CPU over in-memory counters with no
awaits, so an asyncio user can call `calibrate()` from their own `asyncio.TaskGroup` loop and
needs nothing from us to do it.

### D-19 — The hedge quantile times attempts, not executions

Found at phase 6 by reading `hedgeexecutor.go` rather than the doc page. Go starts its timer
*inside* each goroutine and records `time.Since(startTime)` for that attempt alone, and only
when the outcome is acceptable.

Recording the hedged execution's total elapsed time instead is a feedback loop: a hedged
execution spans the stalled primary *plus* the winning attempt, so every hedge teaches the
estimate a larger number, the delay grows, and the policy hedges less and less — exactly when
the tail is worst. The first implementation here had that bug; the regression test
`test_the_quantile_times_each_attempt_not_the_whole_execution` fails if it returns.

This is why `_hedged_attempt` returns `(outcome, execution)` rather than just the outcome: the
winning attempt's own `elapsed_attempt` is not recoverable from the parent execution.

---

## 6. Public API

```python
from failsafe import (
    Failsafe, AsyncFailsafe, current_execution,
    RetryPolicy, CircuitBreaker, Timeout, Fallback, Bulkhead, RateLimiter,
    AdaptiveThrottler, HedgePolicy, CachePolicy, Budget,
    Fixed, Backoff, RandomDelay, Jitter, Ratio, Rate, Result, Condition,
    RetriesExceeded, CircuitBreakerOpen, TimeoutExceeded, BulkheadFull,
    RateLimitExceeded, ThrottleExceeded, BudgetExceeded,
)

retry = RetryPolicy(
    handle=ConnectionError,
    delay=Backoff(1.0, 30.0, jitter=Jitter(factor=0.25)),
    max_retries=3,
    on_retry=lambda e: log.warning("retry %d: %s", e.attempts, e.last_exception),
)
breaker = CircuitBreaker(
    failure_threshold=Rate(0.5, min_executions=20, period=60.0),
    success_threshold=3,
    delay=Fixed(60.0, jitter=Jitter(factor=0.2)),
)

# one composition per backend, reused across every call to it (D-4)
payments = Failsafe(Fallback(value=CACHED), retry, breaker, Timeout(10.0))
conn = payments.call(connect, host)

apayments = AsyncFailsafe(Fallback(value=CACHED), retry, breaker, Timeout(10.0))
conn = await apayments.call(aconnect, host)   # same policy objects, shared state

@payments
def charge(amount): ...

def connect(host):                            # no parameter threading
    ex = current_execution()
    log.info("attempt %d, elapsed %.1fs", ex.attempts, ex.elapsed)
    return do_io(timeout=ex.remaining())      # deadlines, not cancellation, in sync mode

bulkhead.acquire_permit()                     # standalone permits: plain methods (D-4)
try:
    ...
finally:
    bulkhead.release_permit()
```

HTTP, via the optional `failsafe-py[httpx]` extra:

```python
import httpx
from failsafe.httpx import FailsafeTransport, is_retryable, retry_policy, retry_after_delay

# a Response is the execution result, so conditions inspect it rather than compare it:
# Result(429) would ask whether the response *equals* 429, which it never does
breaker = CircuitBreaker(handle=is_retryable, delay=retry_after_delay)

# retry_policy() knows which HTTP failures are worth retrying, and honours Retry-After
client = httpx.Client(transport=FailsafeTransport(retry_policy(max_retries=3), breaker))
```


Adaptive concurrency and prioritized load shedding:

```python
from failsafe import (
    AdaptiveLimiter, AdaptiveThrottler, Failsafe, Priority, UsageTracker,
    priority_scope, queue_prioritizer, user_scope,
)

# discovers the backend's real capacity instead of enforcing a number you guessed
limiter = AdaptiveLimiter()
payments = Failsafe(limiter)

# shed load by priority during overload, coordinated across policies
prioritizer = queue_prioritizer(usage_tracker=UsageTracker())
limiter = AdaptiveLimiter(queueing=(2.0, 3.0)).prioritized_by(prioritizer)
throttler = AdaptiveThrottler().prioritized_by(prioritizer)

# priority is ambient, so it propagates without threading a parameter (D-11)
with priority_scope(Priority.HIGH), user_scope(account_id):
    payments.call(charge, amount)

# the prioritizer only recomputes when asked; the caller owns the schedule (D-18)
with prioritizer.scheduled_calibrations(1.0):
    serve_forever()
```

`Execution` (from Go's `ExecutionInfo` / `ExecutionAttempt`): `attempts`, `executions`, `retries`,
`hedges`, `start_time`, `elapsed`, `attempt_start_time`, `elapsed_attempt`, `last_result`,
`last_exception`, `is_first_attempt`, `is_retry`, `is_hedge`, `deadline`, `remaining()`. In async
mode cancellation surfaces as `CancelledError`; there is no `cancelled` flag (D-5).

---

## 7. Internal architecture

```
Failsafe.call(fn)
  └─ build Execution, compose policy generators inner→outer   (Go: executor.execute)
     └─ pump the outermost generator, servicing instructions:
          Invoke()     → call fn, capture Outcome        (catches Exception, never BaseException)
          Sleep(d)     → time.sleep | asyncio.sleep      (interruptible by cancellation)
          Acquire(...) → FifoSemaphore.acquire{,_async}  (one instance serves both modes)
          Race(g, t)   → worker thread + timer | asyncio.timeout
          Hedge(...)   → concurrent tasks + asyncio.wait (async only; losers cancelled)
     └─ unwrap the final Outcome: return value or raise
```

Shared mutable policy state (breaker windows, limiter buckets, budget counters) lives in the
policy object behind a `threading.Lock` held for microseconds — which is what makes one instance
safe for simultaneous sync and async users.

Generators must release permits when a driver abandons an execution; `try/finally` around
`yield from` handles this (`gen.close()` releases the bulkhead permit).

Deliberately **not** ported: `policyAnyWrapper` / `executionAnyWrapper` (generics erasure makes
them moot), `ExecutionResult` (D-9), `ToExecutor` type tokens, `Run`/`Get` duplication.

---

## 8. Subtle Go semantics that must survive the port

Behaviours found by reading the Go implementation that are easy to miss and easy to get wrong.
Each is a porting requirement.

1. **Half-open admits a bounded number of trials.** `halfOpenState.permittedExecutions` caps
   concurrent trial executions at the success-thresholding capacity; the cap refills as results
   are recorded, and a single failed trial re-opens the circuit. An implementation that merely
   checks `state == "half-open"` lets every waiting caller through at once and stampedes the
   service that is trying to recover.
2. **Retry delay is clamped to the remaining max-duration budget**
   (`adjustForMaxDuration`: `delay = max(0, min(delay, max_duration - elapsed))`), so a long
   backoff never overshoots `max_duration`.
3. **Exponential backoff tracks `lastDelay` per execution, not on the policy** — policies are
   shared and concurrency-safe, so backoff state must live in per-execution scope. In the
   generator design that is the generator's local scope, which gets it right for free.
4. **A result-only failure condition does not replace the default error handling**
   (`BaseFailurePolicy.errorsChecked`): a `handle=Result(...)` condition alone leaves "any error
   is a failure" in place. Conditions are OR'ed.
5. **Timeout overwrites an already-recorded result.** Go deliberately lets a timeout replace a
   completed inner result, because outer policies may still be processing it and must be
   interrupted.
6. **A `Timeout` can be retried by an outer `RetryPolicy`; a context cancellation cannot.** This
   distinction is why `Timeout` exists alongside deadlines: `TimeoutExceeded` is a normal failure,
   `CancelledError` is terminal.
7. **Bulkhead releases its permit per *attempt*** (`PostExecute`), so an inner retry re-acquires.
   The generator must release in `finally` for the abandonment case.
8. **Hedges count as attempts** (`CopyForHedge` increments both `attempts` and `hedges`), and the
   winning attempt cancels the losers with its own result rather than a cancellation error.
9. **The adaptive throttler's window is bucketed at 20 buckets, the circuit breaker's at 10**
   (`NewTimedStats(20, ...)` vs `defaultBucketCount`). Both expire buckets on *write* only, so a
   window nobody has written to still reports its last totals; a read-side expiry would be a
   silent behavioural change.
10. **The throttler's `executionPadding` of 1** (`excess / (executions + 1)`) keeps the rejection
    rate strictly below 1 at low execution counts, so a handful of failures cannot wedge a
    resource shut. `max_rejection_rate` then caps it, defaulting to `.9` so a recovering backend
    always receives probe traffic.
11. **Rate limiter arithmetic runs in integer nanoseconds.** Go's `time.Duration` is an
    `int64` of nanoseconds, and the reservation maths leans on exact integer division:
    `RoundDown(current, interval)` for smooth, `current / period` for bursty. In float
    seconds `0.3 / 0.1` lands just under 3, silently dropping a period. The port keeps a
    nanosecond integer internally and converts at the boundary.
12. **A smooth limiter reserves the *interval before* the permit, not after.** The wait is
    `next_free - now - interval`, so the first caller of an idle limiter waits zero while
    still pushing `next_free` a full interval out. Dropping the `- interval` term makes
    every caller wait one interval too many, including the first.
13. **Bursty available permits go negative.** A burst of 10 against a limit of 2 leaves a
    deficit of -8, and later callers wait several periods for it to drain. `additional_units
    == 0` then decrements `additional_periods`, because a caller using none of a period's
    permits must not wait for it.
14. **`max_wait_time` is executor-only.** Go's `AcquirePermit` blocks indefinitely, while the
    executor calls `AcquirePermitWithMaxWait(ctx, maxWaitTime)`. A standalone caller that
    wants a bound uses `try_reserve_permit(max_wait)`. Preserved deliberately; it reads like
    an inconsistency until you see both call sites.
15. **`httpx.MockTransport` cannot witness a broken request-body rewind.** Its
    `handle_request` calls `request.read()` *before* the handler, which buffers a streaming
    body back onto the request and makes the retry succeed whether or not the transport
    rewound anything. Body-rewind tests use a plain `BaseTransport` that drains
    `request.stream`. A mutation test (remove the buffering, watch the second attempt send
    `b""`) is the only proof that the test is load-bearing.
16. **The limiter's quantile window is a *batch* over one sampling period, not a stream.**
    `record` resets it after every limit update, so `recentWindowMinSamples` (50) is roughly
    the window size. This is what makes an exact sorted window beat a t-digest here; see
    section 2.
17. **The limit is only real if the semaphore follows it.** `updateLimit` calls
    `semaphore.SetSize(newLimit)` as well as storing the float. Porting the arithmetic without
    the resize yields a limiter whose reported limit moves and whose actual concurrency never
    does — `test_the_semaphore_follows_the_limit` pins it.
18. **A shrinking semaphore must not hand a released permit to a waiter.** Go's `Release`
    decrements, *then* wakes only if `used < size`. A handover-first implementation is correct
    at a fixed size and permanently over-admits after a shrink, which is exactly what the
    adaptive limiter does on every decrease. Found by porting the limiter, pinned by
    `test_releasing_after_a_shrink_drains_to_the_new_size`, and mutation-verified.
19. **A prioritized limiter with exactly one registered policy ignores the threshold first.**
    `CanAcquirePermitWithLevel` short-circuits on local capacity when it is the only policy, so
    it need not wait for the next calibration. With two or more it defers entirely to the shared
    rejection threshold — tests that want threshold behaviour must register a second policy.
20. **Levels are recorded only while the limiter is full.** The level distribution that sets the
    rejection threshold must describe contention; recording during idle traffic would bias the
    threshold toward levels that were never under pressure.

---

## 9. Testing strategy

Go uses `testing/synctest` for a virtual clock; the equivalent here is an injected one.

- **One time seam** in `_core` (`clock()` plus the drivers' sleep). Tests install a `FakeClock`
  that advances instantly: no `time.sleep` on the clock seam or in assertion paths, no flaky CI.
  This seam is required by the design, not bolted on for tests.
- **Port failsafe-go's test corpus where it encodes semantics** — `circuitbreaker` (4 files),
  `ratelimiter` (4), `retrypolicy` (2), `test/issues/`. Those tests are the specification; for
  anything ambiguous, cross-check against Go behaviour rather than inventing.
- Concurrency tests (semaphore fairness, hedge cancellation) need real threads and a real loop.
  Keep them few, bounded and deterministic, driven by explicit handoff events rather than sleeping
  and hoping — a "still blocked at t=0.1 s" check is a negative observation and weak evidence.
- **No test reads a wall clock, and none sleeps to wait for another thread.** A sleep in a test
  is a missing seam: either the production code cannot say when it reached a state, or the test
  is guessing at a duration. Both are bugs, and the second is flaky by construction — a
  `>= 0.02` elapsed-time assertion failed CI on a 25 µs margin. The seams that replaced them:
  `FifoSemaphore.await_waiters`/`await_released` (and the `Bulkhead` and `AdaptiveLimiter`
  forwards) block until a caller has parked or a permit has come back, and
  `Prioritizer.on_calibrated` fires on every calibration rather than only on a threshold move.
  Work that must be slow is a `threading.Event` the test releases, not a duration. Removing the
  sleeps halved the suite, from 1.4 s to 0.7 s.
- **The clock seam has two documented holes, both structural.** `asyncio.timeout` runs on the
  event loop's own clock, which no injected clock can drive, so async deadline tests use real
  sub-100 ms waits and `_async._race` translates loop time onto the configured clock for
  `Execution.remaining()`. Interrupting sync timeouts wait on a real OS thread via
  `concurrent.futures.wait`, and a `FakeClock` cannot make another thread finish, so that wait
  is deliberately wall-clock too.
- **Randomness is a seam like time.** Every policy whose behaviour is probabilistic takes an
  `rng: random.Random` alongside `clock=`: `RetryPolicy` and `CircuitBreaker` for delay jitter
  and `RandomDelay`, `AdaptiveThrottler` for its rejection draw. Tests seed it or supply a fixed
  sequence rather than seeding the global module, which turns Go's loop-until-it-rejects tests
  (documented there as 99.99% reliable) into exact assertions.

---

## 10. Roadmap

| Phase | Content | Status |
|---|---|---|
| -1 | Repo skeleton, licence, AGENTS.md, this plan | **done** |
| 0 | CI (ruff + mypy strict + pytest on 3.11–3.14), `_core`, `Execution`, `Outcome`, both drivers, fake clock | **done** |
| 1 | `RetryPolicy`, `Fallback`, `Timeout`, `CircuitBreaker`, composition, events | **done** — Go's retry + breaker corpus ported and passing |
| 2 | `AdaptiveThrottler` | **done** — Go's throttler corpus ported and passing |
| 3 | `RateLimiter`, `Bulkhead`, `failsafe.httpx` client | **done** — Go's rate limiter and bulkhead corpora ported, body rewind pinned by mutation test |
| 4 | `failsafe-py.dev` site + API reference on Read the Docs | **deferred** — skipped at the user's call; nothing in phase 5 depended on it |
| 5 | `AdaptiveLimiter`, `priority`, prioritized variants | **done** — Go's limiter, priority and prioritizer corpora ported; quantile claim measured, not assumed |
| 6 | `HedgePolicy` (async only), `CachePolicy`, `Budget` | **done** — Go's hedge, cache and budget corpora ported; per-attempt quantile timing pinned by mutation test |
| 7 | Server-side HTTP: ASGI middleware, priority headers | post-`1.0` |
| — | gRPC | out of scope |

**Phase 4 — docs, deferred.** Skipped at the user's call during phase 5; nothing in phase 5
depended on it. Port the `failsafe-go.dev` structure page-for-page; the layout is good and the
examples translate almost 1:1. Includes the Go→Python mapping page and the deviations from
`PORT.md`. failsafe and failsafe-go have *different* logos, so failsafe-py gets its own variation
in Python's palette rather than reusing either.

**The source pages are not all current.** `execution-budgets.md` advertises `RetryRate` and
`HedgeRate` metrics that the Go `Budget` interface does not declare at the revision ported
here — it has a single combined `Rate()`. Verify each page against the Go source rather than
translating it, and expect more of these.

**Repos:** two — code and docs separate, mirroring the `failsafe-go` / `failsafe-go.dev` split.

---

## 11. Deferred decisions

Not open questions — decisions deliberately postponed until there is evidence to make them with.

- **`AdaptiveLimiter` configuration shape** (9 tuning parameters): **settled at phase 5 — stays
  flat.** See D-17.
- **Named registry for `Failsafe` instances** (D-4): on the shelf. If the module-scope line proves
  annoying in practice, adding `Failsafe.for_backend(...)` breaks nothing.
