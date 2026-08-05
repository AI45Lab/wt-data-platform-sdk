"""SDK-owned time helpers."""

import time


def now_ms() -> int:
    """Return the current Unix epoch time in milliseconds."""
    return time.time_ns() // 1_000_000
