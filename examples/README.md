# Examples

Runnable programs, each one file. They print what the policies are doing, so the
output is the explanation.

```bash
uv run --extra httpx examples/httpx_retry.py
```

Every example serves its own HTTP endpoint on localhost, so none of them touch the
network and all are reproducible offline.

| Example | Shows |
|---|---|
| [`httpx_retry.py`](httpx_retry.py) | retrying a flaky endpoint through `FailsafeTransport`, honouring `Retry-After` |
| [`httpx_circuit_breaker.py`](httpx_circuit_breaker.py) | a breaker opening on a failing backend, then recovering |
| [`basics.py`](basics.py) | composing retry, timeout and fallback around a plain function |
| [`async_hedge.py`](async_hedge.py) | cutting tail latency with a hedge, and cancelling the loser |
