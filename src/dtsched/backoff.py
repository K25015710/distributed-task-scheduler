"""Exponential backoff with jitter.

After the n-th failed attempt a task waits roughly "base * 2**(n-1)" seconds,
capped at "cap", before it becomes eligible to run again. Jitter spreads the
retries out so that many tasks failing at once (say, because a downstream
service blipped) don't all hammer it again at exactly the same moment.
"""

from __future__ import annotations

import random
from collections.abc import Callable


def backoff_delay(
    attempt: int,
    base: float = 1.0,
    cap: float = 60.0,
    *,
    jitter: bool = True,
    rand: Callable[[], float] = random.random,
) -> float:
    """Return the delay in seconds before retrying after failed attempt "attempt".

    "attempt" is 1-based: the delay after the first failure is about "base".
    With "jitter" the delay is drawn uniformly from "[d/2, d]" ("equal
    jitter"), which keeps a guaranteed minimum wait while still de-synchronising
    retries.
    """
    if attempt < 1:
        raise ValueError("attempt must be >= 1")
    if base < 0 or cap < 0:
        raise ValueError("base and cap must be non-negative")
    # Cap the exponent too, so a huge attempt number can't overflow a float.
    delay = min(cap, base * 2 ** min(attempt - 1, 62))
    if jitter:
        delay = delay / 2 + rand() * delay / 2
    return delay
