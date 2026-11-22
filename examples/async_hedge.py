"""Cut tail latency with a hedge, and cancel the attempt that lost.

The first call stalls. Rather than wait it out, the policy starts a second attempt
after 50ms and takes whichever finishes first; the loser is cancelled for real.

Hedging is async only: cancelling a synchronous call is not possible, so
``Failsafe`` raises ``NotImplementedError`` for this policy.

    uv run examples/async_hedge.py
"""

from __future__ import annotations

import asyncio
import itertools

from failsafe import AsyncFailsafe, HedgePolicy

attempt = itertools.count(1)
cancelled: list[int] = []


async def slow_first_call() -> str:
    """Stalls on the first attempt; answers immediately on any hedge."""
    n = next(attempt)
    if n == 1:
        print(f"  attempt {n}: stalled")
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled.append(n)
            raise
        return "slow answer"
    print(f"  attempt {n}: answered")
    return f"answer from attempt {n}"


async def main() -> None:
    api = AsyncFailsafe(HedgePolicy(0.05))

    print("Calling a backend whose first attempt stalls:")
    result = await api.call(slow_first_call)

    await asyncio.sleep(0)  # let the cancellation land
    print(f"  got {result!r}")
    print(f"  cancelled attempts: {cancelled}")


if __name__ == "__main__":
    asyncio.run(main())
