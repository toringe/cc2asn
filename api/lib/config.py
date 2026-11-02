"""Configuration for the CC2ASN web API, read from environment variables."""

import os
from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class Config:
    port: int = 8080
    bucket: str = "cc2asn-db"
    table: str = "cc2asn-tokens"
    rate_per_minute: int = 30
    rate_per_day: int = 300
    cache_ttl: int = 300
    bridge_secret: str = ""
    log_group_latest: str = "/cc2asn/api/latest"
    log_group_historic: str = "/cc2asn/api/historic"


_INT_VARS = {
    "PORT": "port",
    "RATE_PER_MINUTE": "rate_per_minute",
    "RATE_PER_DAY": "rate_per_day",
    "CACHE_TTL": "cache_ttl",
}

_STR_VARS = {
    "CC2ASN_DB_BUCKET": "bucket",
    "CC2ASN_TOKENS_TABLE": "table",
    "CC2ASN_BRIDGE_SECRET": "bridge_secret",
    "CC2ASN_LOG_GROUP_LATEST": "log_group_latest",
    "CC2ASN_LOG_GROUP_HISTORIC": "log_group_historic",
}


def load(env: Mapping[str, str] | None = None) -> Config:
    values = dict(os.environ if env is None else env)
    kwargs = {}
    for env_key, attr in _STR_VARS.items():
        if values.get(env_key):
            kwargs[attr] = values[env_key]
    for env_key, attr in _INT_VARS.items():
        if values.get(env_key) is not None:
            kwargs[attr] = int(values[env_key])  # raises ValueError on junk
    return Config(**kwargs)
