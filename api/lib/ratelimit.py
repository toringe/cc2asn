"""In-memory, per-warm-environment rate limiting for anonymous traffic."""

import time
from collections import deque


class RateLimiter:
    def __init__(self, per_minute: int, per_day: int):
        self.per_minute = per_minute
        self.per_day = per_day
        self._buckets = {}

    def allow(self, ip: str, today: str, now: float | None = None) -> bool:
        now = time.time() if now is None else now
        bucket = self._buckets.get(ip)
        if bucket is None:
            bucket = self._buckets.setdefault(ip, {"stamps": deque(), "day": None})

        stamps = bucket["stamps"]
        while stamps and now - stamps[0] > 60:
            stamps.popleft()
        if len(stamps) >= self.per_minute:
            return False

        day = bucket["day"]
        if day is None or day[0] != today:
            day = (today, 0)
            bucket["day"] = day
        elif day[1] >= self.per_day:
            return False

        stamps.append(now)
        bucket["day"] = (day[0], day[1] + 1)
        return True
