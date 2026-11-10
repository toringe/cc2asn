"""Per-query log writing to the dedicated CloudWatch query log group.

Latest-day lookups -> /cc2asn/api/latest (legacy whois line format).
(The /cc2asn/api/historic group is no longer written; historic lookups were
withdrawn 2026-11-10.) Operational events stay in the function's own group; failures here must
never break a served response (the server wraps writes defensively).
"""

import datetime
from time import localtime, strftime, time  # noqa: F401  (kept minimal import surface)


def legacy_line(
    ts: datetime.datetime, ip: str, rectype: str, cc: str, src: str | None = None
) -> str:
    if ts.tzinfo is not None:
        ts = ts.astimezone(datetime.timezone.utc)
    line = ts.strftime("%Y-%m-%d %H:%M:%S") + f" {ip} {rectype} {cc}"
    if src:
        line += f" src={src}"
    return line


class QueryLog:
    def __init__(self, group_name: str, client=None):
        self.group_name = group_name
        self._client = client
        self._streams = set()

    @property
    def client(self):
        if self._client is None:
            import boto3
            from botocore.config import Config

            # Bounded timeouts: the function allows 10s total; a hung Logs
            # endpoint must degrade to a served response plus a warning,
            # never to event-loop blockage.
            self._client = boto3.client(
                "logs",
                config=Config(
                    connect_timeout=1,
                    read_timeout=2,
                    retries={"max_attempts": 1},
                ),
            )
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
        except self.client.exceptions.ResourceNotFoundException:
            # Group missing (fresh deployment); recover once.
            self.client.create_log_group(logGroupName=self.group_name)
            self.client.create_log_stream(
                logGroupName=self.group_name, logStreamName=stream_name
            )
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
