"""Reads per-country data files from S3 with a TTL cache for the latest/ set."""

import time
from datetime import date as _date  # noqa: F401  (kept for typing clarity)

from lib.whois import Query  # noqa: F401  (typing of callers)


def latest_key(rectype: str, cc: str) -> str:
    return f"latest/{cc}_{rectype}"


def historic_key(date: str, rectype: str, cc: str) -> str:
    y, m, d = date.split("-")
    return f"{y}/{int(m):02d}/{int(d):02d}/{cc}_{rectype}"


class DataStore:
    def __init__(self, bucket: str, cache_ttl: int = 300, client=None):
        self.bucket = bucket
        self.cache_ttl = cache_ttl
        self._client = client
        self._cache = {}
        self.last_source = None  # "cache" | "s3" | None (after last get_latest)

    def _now(self) -> float:
        return time.time()

    @property
    def client(self):
        if self._client is None:
            import boto3

            self._client = boto3.client("s3")
        return self._client

    def _fetch(self, key: str) -> str | None:
        client = self.client
        try:
            with client.get_object(Bucket=self.bucket, Key=key)["Body"] as body:
                data = body.read()
        except (client.exceptions.NoSuchKey, client.exceptions.AccessDenied):
            # AccessDenied doubles as "no such key" here: without s3:ListBucket,
            # S3 cannot confirm a key's existence and answers a GET of a
            # nonexistent object with 403 instead of NoSuchKey (verified live
            # 2026-11-03 via the /NV traceback). Other client errors (bad
            # credentials, throttling, wrong bucket) still propagate as 500.
            return None
        return data.decode("utf-8")

    def get_latest(self, rectype: str, cc: str) -> str | None:
        key = latest_key(rectype, cc)
        cached = self._cache.get(key)
        if cached is not None:
            fetched_at, body = cached
            if self._now() - fetched_at <= self.cache_ttl:
                self.last_source = "cache"
                return body
        body = self._fetch(key)
        self.last_source = "s3" if body is not None else None
        if body is not None:
            self._cache[key] = (self._now(), body)
        return body

    def get_historic(self, date: str, rectype: str, cc: str) -> str | None:
        return self._fetch(historic_key(date, rectype, cc))
