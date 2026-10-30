"""In-memory, per-warm-environment rate limiting for anonymous traffic."""

import time
from collections import deque


class RateLimiter:
    def __init__(self, per_minute: int, per_day: int):
        self.per_minute = per_minute
        self.per_day = per_day
        self._buckets = {}
        self._since_sweep = 0

    def allow(self, ip: str, today: str, now: float | None = None) -> bool:
        now = time.time() if now is None else now
        self._since_sweep += 1
        if self._since_sweep >= 128:
            self._since_sweep = 0
            self._sweep(now, today)
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

    def _sweep(self, now: float, today: str) -> None:
        """Evict idle buckets: no in-window stamps and the day counter is stale.

        Same-date (possibly exhausted) buckets stay, so eviction cannot be
        abused to reset a daily cap.
        """
        for ip in list(self._buckets):
            bucket = self._buckets[ip]
            stamps = bucket["stamps"]
            while stamps and now - stamps[0] > 60:
                stamps.popleft()
            if not stamps and (bucket["day"] is None or bucket["day"][0] != today):
                del self._buckets[ip]
