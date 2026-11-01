"""Per-query log writing to the two dedicated CloudWatch query log groups.

Free latest-day lookups -> /cc2asn/api/latest  (legacy whois line format).
Historic lookups        -> /cc2asn/api/historic (legacy prefix + enrichment).
Operational events stay in the function's own group; failures here must
never break a served response (the server wraps writes defensively).
"""

import datetime
from time import localtime, strftime, time  # noqa: F401  (kept minimal import surface)


def legacy_line(ts: datetime.datetime, ip: str, rectype: str, cc: str) -> str:
    if ts.tzinfo is not None:
        ts = ts.astimezone(datetime.timezone.utc)
    return ts.strftime("%Y-%m-%d %H:%M:%S") + f" {ip} {rectype} {cc}"


def historic_line(
    ts: datetime.datetime,
    ip: str,
    rectype: str,
    cc: str,
    date: str,
    ms: int,
    source: str,
    tier: str,
    tok_prefix: str,
) -> str:
    return (
        legacy_line(ts, ip, rectype, cc)
        + f" date={date} status=200 ms={ms} src={source} tier={tier} tok={tok_prefix}"
    )


class QueryLog:
    def __init__(self, group_name: str, client=None):
        self.group_name = group_name
        self._client = client
        self._streams = set()

    @property
    def client(self):
        if self._client is None:
            import boto3

            self._client = boto3.client("logs")
        return self._client

    def _ensure_stream(self, stream_name: str) -> None:
        if stream_name in self._streams:
            return
        try:
            self.client.create_log_stream(
                logGroupName=self.group_name, logStreamName=stream_name
            )
        except self.client.exceptions.ResourceAlreadyExistsException:
            pass  # another environment created it first
        self._streams.add(stream_name)

    def write(self, line: str, stream_date: str, now_ms: int | None = None) -> bool:
        """One line into `group/stream_date`; stream auto-created per day."""
        self._ensure_stream(stream_date)
        timestamp = int(time() * 1000) if now_ms is None else now_ms
        self.client.put_log_events(
            logGroupName=self.group_name,
            logStreamName=stream_date,
            logEvents=[{"timestamp": timestamp, "message": line}],
        )
        return True
