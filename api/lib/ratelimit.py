"""Rate limiting for anonymous traffic.

RateLimiter: in-memory, per warm environment. A cheap first line against
bursts (per-minute) and the daily fallback when DynamoDB is unavailable.

DailyLimit: the authoritative per-client daily cap, one atomic counter per
client per UTC day in DynamoDB, shared by all warm environments. In-memory
counters alone cannot enforce a daily maximum: Lambda recycles idle
environments, which resets them.
"""

import datetime
import hashlib
import ipaddress
import logging
import time
from collections import deque

logger = logging.getLogger("cc2asn.api")


def client_key(ip: str) -> str:
    """Stable, pseudonymous key for one user/device.

    IPv6 clients are keyed on their /64: a single device rotates through
    privacy addresses inside it. IPv4-mapped IPv6 counts as the IPv4 address.
    The IP is hashed so the table does not hold plain addresses.
    """
    raw = ip.strip()
    try:
        addr = ipaddress.ip_address(raw)
    except ValueError:
        norm = raw
    else:
        if addr.version == 6 and addr.ipv4_mapped is not None:
            norm = str(addr.ipv4_mapped)
        elif addr.version == 6:
            norm = str(ipaddress.ip_network(f"{addr}/64", strict=False))
        else:
            norm = str(addr)
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()[:32]


class DailyLimit:
    """Per-client daily cap shared across environments via DynamoDB.

    Items live in the existing token table under `rl#<YYYY-MM-DD>#<client_key>`
    with an `expires_at` epoch (two days on) for DynamoDB TTL cleanup.
    One conditional write per allowed query. Once a client is refused, this
    environment remembers it for the rest of the day and stops writing.
    Any DynamoDB failure fails open (logged): the free service must not go
    down with the counter store; RateLimiter still bounds each environment.
    """

    def __init__(self, table, per_day: int, ttl_days: int = 2):
        self.table = table
        self.per_day = per_day
        self.ttl_days = ttl_days
        self._exhausted = set()
        self._exhausted_day = None

    def allow(self, ip: str, today: str) -> bool:
        if self._exhausted_day != today:
            self._exhausted = set()
            self._exhausted_day = today
        key = f"rl#{today}#{client_key(ip)}"
        if key in self._exhausted:
            return False
        day = datetime.date.fromisoformat(today)
        expires = datetime.datetime.combine(
            day + datetime.timedelta(days=self.ttl_days),
            datetime.time(),
            tzinfo=datetime.timezone.utc,
        )
        conditional_failed = self.table.meta.client.exceptions.ConditionalCheckFailedException
        try:
            self.table.update_item(
                Key={"tk": key},
                UpdateExpression="SET expires_at = :exp ADD #n :one",
                ConditionExpression="attribute_not_exists(#n) OR #n < :limit",
                ExpressionAttributeNames={"#n": "n"},
                ExpressionAttributeValues={
                    ":one": 1,
                    ":limit": self.per_day,
                    ":exp": int(expires.timestamp()),
                },
            )
            return True
        except conditional_failed:
            self._exhausted.add(key)
            return False
        except Exception:
            logger.warning("Daily limit store unavailable; allowing", exc_info=True)
            return True


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

    def daily_exhausted(self, ip: str, today: str) -> bool:
        """True when this environment's daily counter for `ip` is used up."""
        bucket = self._buckets.get(ip)
        if bucket is None or bucket["day"] is None:
            return False
        day, count = bucket["day"]
        return day == today and count >= self.per_day

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
