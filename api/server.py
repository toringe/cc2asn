"""CC2ASN WHOIS-WEB API: a stdlib HTTP server exposing cc2asn-db data."""

import logging
import os
from dataclasses import dataclass
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from lib.config import Config, load
from lib.ratelimit import RateLimiter
from lib.s3data import DataStore, historic_key, latest_key
from lib.tokens import TokenStore
from lib.whois import parse_query, render_json, render_plain

logger = logging.getLogger("cc2asn.api")

BANNER = (
    "CC2ASN WHOIS-WEB API. Query: /{CC} or /{TYPE}/{CC}. "
    "Historic: /{YYYY-MM-DD}/... (token required).\n"
)


@dataclass
class Deps:
    config: Config
    store: DataStore
    tokens: TokenStore
    limiter: RateLimiter


def build_deps(config: Config) -> Deps:
    import boto3

    store = DataStore(bucket=config.bucket, cache_ttl=config.cache_ttl)
    table = boto3.resource("dynamodb").Table(config.table)
    return Deps(
        config=config,
        store=store,
        tokens=TokenStore(table),
        limiter=RateLimiter(per_minute=config.rate_per_minute, per_day=config.rate_per_day),
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

    def _client_ip(self) -> str:
        # Behind the adapter every TCP peer is the local proxy; the real client
        # arrives in X-Forwarded-For. Verify exact adapter header semantics at
        # deployment (follow-up of the invoke-mode open item).
        forwarded = self.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()
        return self.client_address[0]

    def _route_get(self):
        today = date.today().isoformat()
        path = self.path
        if path.split("?", 1)[0] == "/":
            self._send(200, BANNER, "text/plain; charset=utf-8")
            return

        query = parse_query(path)
        if query is None:
            self._send(404, f"Not found: {path}\n", "text/plain; charset=utf-8")
            return

        want_json = "format=json" in self.path.split("?", 1)[-1] if "?" in self.path else False
        data_date = query.date or "latest"

        body = None
        if query.date is None:
            if not self.deps.limiter.allow(self._client_ip(), today):
                self._send(
                    429,
                    "Rate limit exceeded\n",
                    "text/plain; charset=utf-8",
                    {"Retry-After": "60"},
                )
                return
            body = self.deps.store.get_latest(query.rectype, query.cc)
            if body is None:
                self._send(
                    404,
                    f"Missing data: {latest_key(query.rectype, query.cc)}\n",
                    "text/plain; charset=utf-8",
                )
                return
        else:
            token = self._bearer_token()
            item = self.deps.tokens.authorize(token, today) if token else None
            if item is None:
                self._send(
                    401,
                    "Unauthorized\n",
                    "text/plain; charset=utf-8",
                    {"WWW-Authenticate": "Bearer"},
                )
                return
            if not self.deps.tokens.consume(token, today):
                self._send(429, "Token quota exhausted\n", "text/plain; charset=utf-8")
                return
            body = self.deps.store.get_historic(query.date, query.rectype, query.cc)
            if body is None:
                key = historic_key(query.date, query.rectype, query.cc)
                self._send(
                    404,
                    f"Missing data: {key}\n",
                    "text/plain; charset=utf-8",
                )
                return

        headers = {"X-Data-Date": data_date}
        if want_json:
            self._send(200, render_json(query, data_date, body), "application/json", headers)
        else:
            self._send(200, render_plain(body), "text/plain; charset=utf-8", headers)

    def _bearer_token(self) -> str | None:
        header = self.headers.get("Authorization")
        if not header:
            return None
        parts = header.split(" ")
        if len(parts) != 2 or parts[0].lower() != "bearer":
            return None
        return parts[1]

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
