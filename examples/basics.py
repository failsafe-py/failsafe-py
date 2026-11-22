"""Compose retry, timeout and fallback around a plain function.

Policies are applied outermost first, so the fallback sees whatever the retry could
not rescue.

    uv run examples/basics.py
"""

from __future__ import annotations

import itertools

from failsafe import Backoff, Failsafe, Fallback, RetryPolicy, Timeout

attempt = itertools.count(1)


def flaky_lookup(key: str) -> str:
    """Fails twice, then succeeds."""
    n = next(attempt)
    print(f"  attempt {n}")
    if n < 3:
        raise ConnectionError("backend unreachable")
    return f"{key}=42"


def always_fails(key: str) -> str:
    raise ConnectionError("backend is down")


def main() -> None:
    retry = RetryPolicy(max_retries=3, delay=Backoff(0.1, 1.0))

    print("A flaky call, retried until it succeeds:")
    print(" ", Failsafe(retry, Timeout(5.0)).call(flaky_lookup, "answer"))

    print("\nThe same policies, but the backend never recovers:")
    guarded = Failsafe(Fallback(value="answer=cached"), retry, Timeout(5.0))
    print(" ", guarded.call(always_fails, "answer"))


if __name__ == "__main__":
    main()
