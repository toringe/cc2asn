"""CC2ASN WEB API: a stdlib HTTP server exposing cc2asn-db data."""

import json
import hmac
import logging
import datetime
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from lib.config import Config, load
from lib.querylog import legacy_line
from lib.ratelimit import DailyLimit, RateLimiter
from lib.s3data import DataStore
from lib.whois import parse_query, render_json, render_plain

logger = logging.getLogger("cc2asn.api")

BANNER = (
    "CC2ASN WEB API. Query: /{CC} or /{TYPE}/{CC} "
    "(TYPE: ASN, IPV4, IPV6, ALL). Latest day only, free, max 100 queries per day.\n"
)

HISTORIC_GONE = (
    "Historic lookups are not available. Query: /{CC} or /{TYPE}/{CC}\n"
)


def utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def seconds_until_utc_midnight(now: datetime.datetime) -> int:
    tomorrow = datetime.datetime.combine(
        now.date() + datetime.timedelta(days=1), datetime.time(), tzinfo=datetime.timezone.utc
    )
    return max(1, int((tomorrow - now).total_seconds()))


@dataclass
class Deps:
    config: Config
    store: DataStore
    limiter: RateLimiter                # in-memory, per environment: bursts + fallback
    daily: object | None = None         # DailyLimit: shared per-client daily cap
    latest_log: object | None = None    # QueryLog for latest-day lookups


def build_deps(config: Config) -> Deps:
    import boto3

    from lib.querylog import QueryLog

    store = DataStore(bucket=config.bucket, cache_ttl=config.cache_ttl)
    table = boto3.resource("dynamodb").Table(config.table)
    return Deps(
        config=config,
        store=store,
        limiter=RateLimiter(per_minute=config.rate_per_minute, per_day=config.rate_per_day),
        daily=DailyLimit(table, per_day=config.rate_per_day),
        latest_log=QueryLog(config.log_group_latest),
    )


class WhoisHandler(BaseHTTPRequestHandler):
    deps: Deps

    def _send(self, status: int, body: str, content_type: str, extra_headers=None):
        payload = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        for name, value in (extra_headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        try:
            self._route_get()
        except Exception:
            logger.exception("Request failed: %s", self.path)
            try:
                self._send(500, "Internal error\n", "text/plain; charset=utf-8")
            except OSError:
                pass  # the client connection was already gone

    def _is_bridge(self) -> bool:
        """True for bridge-originated requests: the shared secret matches.

        The bridge sends `Authorization: Bearer <CC2ASN_BRIDGE_SECRET>` plus
        the originating user's IP in X-Forwarded-For. Direct public callers
        can never forge the compare, so XFF stays untrusted for them.
        """
        header = self.headers.get("Authorization")
        secret = self.deps.config.bridge_secret
        if not header or not secret:
            return False
        parts = header.split(" ")
        if len(parts) != 2 or parts[0].lower() != "bearer":
            return False
        return self._matches_accept_list(parts[1], secret)

    @staticmethod
    def _matches_accept_list(token: str, accept_list: str) -> bool:
        # Comma-separated accept-list (secretless-rotation support: during
        # rollover old+new are both trusted). Every entry is compared
        # constant-time; non-ASCII junk (latin-1-decoded headers,
        # attacker-provided or shell junk) degrades to "no match", never a 500.
        try:
            token_b = token.encode("utf-8")
        except UnicodeEncodeError:
            return False
        return any(
            hmac.compare_digest(token_b, s.strip().encode("utf-8"))
            for s in accept_list.split(",") if s.strip()
        )

    def _is_proxy(self) -> bool:
        # CloudFront-fronted requests carry the distribution's fixed origin
        # header as proof of the front door (set in distribution config, not
        # visible to clients). Its own header name — never Authorization —
        # so it cannot collide with the bridge's Bearer secret.
        secret = self.deps.config.proxy_secret
        if not secret:
            return False
        given = self.headers.get("X-CC2ASN-Proxy")
        return bool(given) and self._matches_accept_list(given, secret)

    def _client_ip(self) -> str:
        # Behind the adapter the trusted caller identity arrives in
        # x-amzn-request-context (JSON payload; http.sourceIp) — verified live
        # 2026-11-01. X-Forwarded-For is client-controlled on the public URL
        # and must not be trusted there; the XFF/TCP-peer fallbacks are for
        # local development runs only.
        #
        # Bridge requests are the exception: they carry the end user's IP in
        # XFF behind the bridge secret, and the adapter context would only
        # say "the bridge VM's IP". Bridge XFF resolution deliberately skips
        # the adapter context (the whole point is per-user logging/limits).
        if self._is_bridge():
            forwarded = self.headers.get("X-Forwarded-For")
            if forwarded:
                return forwarded.split(",")[-1].strip()
        # CloudFront-fronted web requests are the other exception: CloudFront
        # appends the viewer IP as the LAST XFF entry (the trusted final
        # hop), and the proxy secret proves the front door. Same resolution
        # shape as the bridge — last entry; both trusts live in _client_ip().
        if self._is_proxy():
            forwarded = self.headers.get("X-Forwarded-For")
            if forwarded:
                return forwarded.split(",")[-1].strip()
        context = self.headers.get("x-amzn-request-context")
        if context:
            try:
                source_ip = json.loads(context).get("http", {}).get("sourceIp")
                if source_ip:
                    return source_ip
            except ValueError:
                pass
        forwarded = self.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()
        return self.client_address[0]

    def _log_query(self, writer, line: str, stream_date: str) -> None:
        """Best-effort per-query log write; never breaks the response."""
        if writer is None:
            return
        try:
            writer.write(line, stream_date=stream_date)
        except Exception:
            logger.warning("Query log write failed", exc_info=True)

    def _route_get(self):
        now = utcnow()
        today = now.date().isoformat()
        path = self.path
        if path.split("?", 1)[0] == "/":
            self._send(200, BANNER, "text/plain; charset=utf-8")
            return

        query = parse_query(path)
        if query is None:
            self._send(404, f"Not found: {path}\n", "text/plain; charset=utf-8")
            return
        if query.date is not None:
            # Historic lookups were withdrawn (2026-11-10); the parser still
            # recognises the form so callers get a clear answer, not "Not found".
            self._send(404, HISTORIC_GONE, "text/plain; charset=utf-8")
            return

        want_json = "format=json" in self.path.split("?", 1)[-1] if "?" in self.path else False

        ip = self._client_ip()
        limiter = self.deps.limiter
        daily = self.deps.daily
        if not limiter.allow(ip, today):
            if limiter.daily_exhausted(ip, today):
                self._send_daily_limited(now)
            else:
                self._send(
                    429,
                    "Rate limit exceeded\n",
                    "text/plain; charset=utf-8",
                    {"Retry-After": "60"},
                )
            return
        if daily is not None and not daily.allow(ip, today):
            self._send_daily_limited(now)
            return

        body = self.deps.store.get_latest(query.rectype, query.cc)
        if body is None:
            self._send(404, f"Not found: {path}\n", "text/plain; charset=utf-8")
            return

        # The query log write MUST happen before the response is sent:
        # PutLogEvents calls placed after _send() were observed to never
        # complete in this runtime/adapter setup (see plan ledger).
        # It is best-effort: failures never break the response.
        self._log_query(
            self.deps.latest_log,
            legacy_line(now, ip, query.rectype, query.cc,
                        src="tcp43" if self._is_bridge() else "http"),
            now.strftime("%Y/%m/%d"),
        )

        headers = {"X-Data-Date": "latest"}
        if want_json:
            self._send(200, render_json(query, "latest", body), "application/json", headers)
        else:
            self._send(200, render_plain(body), "text/plain; charset=utf-8", headers)

    def _send_daily_limited(self, now: datetime.datetime) -> None:
        self._send(
            429,
            f"Daily limit exceeded ({self.deps.config.rate_per_day} queries per day). "
            "Resets at 00:00 UTC.\n",
            "text/plain; charset=utf-8",
            {"Retry-After": str(seconds_until_utc_midnight(now))},
        )

    def do_HEAD(self):
        self._send(405, "Method not allowed\n", "text/plain; charset=utf-8")

    do_POST = do_HEAD
    do_PUT = do_HEAD
    do_DELETE = do_HEAD

    def log_message(self, format, *args):
        logger.info("%s - %s", self.address_string(), format % args)


def create_server(deps: Deps, port: int = 0) -> ThreadingHTTPServer:
    handler = type("BoundWhoisHandler", (WhoisHandler,), {"deps": deps})
    return ThreadingHTTPServer(("0.0.0.0", port), handler)


def main():
    logging.basicConfig(level=logging.INFO)
    config = load()
    server = create_server(build_deps(config), port=config.port)
    logger.info("CC2ASN API listening on port %s", config.port)
    server.serve_forever()


if __name__ == "__main__":
    main()
