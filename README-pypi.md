# failsafe-py

Fault tolerance and resilience policies for Python: wrap a call with one or more policies,
composed in whatever order suits you.

- **Failure handling** — Retry, Fallback
- **Load limiting** — Circuit Breaker, Adaptive Limiter, Adaptive Throttler, Bulkhead, Rate Limiter, Cache
- **Time limiting** — Timeout, Hedge

Works with sync and async callers, and a single policy instance can be shared between them — a
circuit breaker tripped by a blocking call is seen open by an `async` one.

```python
from failsafe import Failsafe, RetryPolicy, CircuitBreaker, Backoff

# one composition per backend, reused across every call to it
payments = Failsafe(
    RetryPolicy(handle=ConnectionError, delay=Backoff(1.0, 30.0), max_retries=3),
    CircuitBreaker(failure_threshold=5, delay=60.0),
)

charge = payments.call(post, "/charge", amount)
```

The same policies work under `AsyncFailsafe`:

```python
from failsafe import AsyncFailsafe

response = await AsyncFailsafe(retry, breaker).call(fetch, url)
```

## Install

```sh
pip install failsafe-py          # zero runtime dependencies
pip install "failsafe-py[httpx]" # plus the httpx transport
```

Python 3.11+. Ships `py.typed`.

## Documentation

- **[Guides and usage](https://failsafe-py.github.io)** — how each policy behaves and how to compose them
- **[API reference](https://failsafe-py.readthedocs.io)** — every public symbol
- **[Porting from failsafe-go](https://github.com/failsafe-py/failsafe-py/blob/main/PORT.md)** — name mapping and deviations

## Project status

A port of [failsafe-go](https://github.com/failsafe-go/failsafe-go), built largely with coding
agents. Pre-1.0: the API may still change, and specific design feedback is welcome via
[GitHub issues](https://github.com/failsafe-py/failsafe-py/issues) — see the
[project status notes](https://github.com/failsafe-py/failsafe-py#project-status) for the author's
own caveats about the approach.

## License

MIT. &copy; 2026-present Nicolas Dumazet and contributors.
