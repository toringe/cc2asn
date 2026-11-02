"""End-to-end whois-protocol tests for the bridge server (bridge/server.py).

Boots the bridge on an ephemeral port against the in-process fake API, connects
real client sockets, and asserts response bytes and connection behavior — the
wire contract a `whois` or `nc` client will see.
"""

import logging
import os
import shutil
import socket
import tempfile
import unittest

from server import make_query_logger, make_server
from test.test_api import FakeAPI, FakeAPIHandler, SECRET


class Query(object):
    """Send a raw whois query to a booted bridge, return all response bytes."""

    def __init__(self, server):
        self.server = server

    def __call__(self, payload, *more):
        with socket.create_connection(self.server.server_address, timeout=10) as sock:
            sock.sendall(payload)
            response = b""
            try:
                while True:
                    chunk = sock.recv(4096)
                    if not chunk:
                        break          # server closed the connection
                    response += chunk
            except (ConnectionResetError, ConnectionAbortedError):
                # Legacy recv(8)+close sends RST when the client had bytes
                # left unread (i.e. the query exceeded 8 bytes). Clients
                # observe silence — same as the legacy server.
                pass
        return response


class BridgeFixture(unittest.TestCase):
    """Shared boot/shutdown of one bridge instance per test."""

    API_ROUTES: dict = {}

    def boot(self, api_base=None, secret=SECRET, querylog=None):
        if api_base is None:
            FakeAPIHandler.routes = dict(self.API_ROUTES)
            api_base = self._fake_api
        self.server = make_server(("127.0.0.1", 0), api_base, secret,
                                  querylog=querylog)
        self.server_thread = __import__("threading").Thread(
            target=self.server.serve_forever, daemon=True)
        self.server_thread.start()
        self.query = Query(self.server)
        self.addCleanup(self.shutdown)
        return self.server

    def shutdown(self):
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join(timeout=5)

    @classmethod
    def setUpClass(cls):
        cls._fake_api_ = FakeAPI()
        cls._fake_api_.__enter__()
        cls._fake_api = cls._fake_api_.base_url

    @classmethod
    def tearDownClass(cls):
        cls._fake_api_.__exit__(None, None, None)


class WireParityTest(BridgeFixture):
    """Data response shaping: what reaches the whois client."""

    API_ROUTES = {
        "/ASN/FI": (200, b"AS1\nAS2\n"),
        "/IPV4/DK": (200, b"1.2.3.0/24\n"),
    }

    def test_bare_country_serves_asn_list(self):
        server = self.boot()
        _ = server
        self.assertEqual(self.query(b"fi\r\n"), b"AS1\nAS2\n")

    def test_typed_query_serves_requested_type(self):
        self.boot()
        self.assertEqual(self.query(b"ipv4 dk\r\n"), b"1.2.3.0/24\n")


class ErrorParityTest(BridgeFixture):
    """Failure shaping: silence like legacy, or one short error line."""

    API_ROUTES = {}

    def test_invalid_query_is_silence(self):
        self.boot()
        self.assertEqual(self.query(b"noo\r\n"), b"")

    def test_query_longer_than_legacy_8_bytes_is_invalid(self):
        self.boot()
        self.assertEqual(self.query(b"ipv4 dkXX"), b"")

    def test_missing_country_is_silence(self):
        # /ASN/ZZ is unset → fake API answers 404, exactly like a country
        # file not being there in the legacy server.
        self.boot()
        self.assertEqual(self.query(b"zz\r\n"), b"")
        self.assertEqual(FakeAPIHandler.last_path, "/ASN/ZZ")

    def test_rate_limited_response_line(self):
        self.boot()
        FakeAPIHandler.routes = {"/ASN/NO": (429, b"limit")}   # after boot: boot() would overwrite
        self.assertEqual(self.query(b"no\r\n"), b"Rate limit exceeded\n")

    def test_api_error_response_line(self):
        self.boot()
        FakeAPIHandler.routes = {"/ASN/NO": (500, b"oops")}
        self.assertEqual(self.query(b"no\r\n"), b"Service unavailable\n")

    def test_dead_api_response_line(self):
        self.boot(api_base="http://127.0.0.1:1")  # nothing listening
        self.assertEqual(self.query(b"no\r\n"), b"Service unavailable\n")


class ForwardingHeadersTest(BridgeFixture):
    """The secret + client-IP contract with the API (per-user rate limit)."""

    API_ROUTES = {"/ASN/FI": (200, b"AS1\n")}

    def test_sends_secret_and_client_ip_to_api(self):
        self.boot()
        self.assertEqual(self.query(b"fi\r\n"), b"AS1\n")
        self.assertEqual(FakeAPIHandler.last_headers.get("Authorization"),
                         "Bearer " + SECRET)
        self.assertEqual(FakeAPIHandler.last_headers.get("X-Forwarded-For"),
                         "127.0.0.1")


class QueryLogTest(BridgeFixture):
    """The local log-file: one legacy-shaped line per request."""

    API_ROUTES = {"/ASN/FI": (200, b"AS1\n")}

    def boot_logged(self):
        tmpdir = tempfile.mkdtemp()
        self.log_path = os.path.join(tmpdir, "query.log")
        self.addCleanup(shutil.rmtree, tmpdir, ignore_errors=True)
        logger = make_query_logger(self.log_path)
        self.addCleanup(logging.shutdown)
        return self.boot(querylog=logger)

    def read_log(self):
        with open(self.log_path) as fh:
            return fh.read()

    def test_successful_query_line_format(self):
        self.boot_logged()
        self.query(b"fi\r\n")
        line = self.read_log().strip()
        self.assertRegex(
            line,
            r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} 127\.0\.0\.1 ASN FI 200$")

    def test_invalid_query_line(self):
        self.boot_logged()
        self.query(b"noo\r\n")
        line = self.read_log().strip()
        self.assertRegex(
            line,
            r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} 127\.0\.0\.1 - - INVALID$")

    def test_rate_limited_query_line(self):
        self.boot_logged()
        FakeAPIHandler.routes = {"/ASN/NO": (429, b"x")}
        self.query(b"no\r\n")
        line = self.read_log().strip()
        self.assertRegex(
            line, r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} 127\.0\.0\.1 ASN NO 429$")

    def test_missing_country_is_still_logged(self):
        self.boot_logged()
        self.query(b"zz\r\n")
        line = self.read_log().strip()
        self.assertRegex(
            line, r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} 127\.0\.0\.1 ASN ZZ 404$")


class IdleConnectionTest(BridgeFixture):
    """Idle connections must be reaped (thread exhaustion hardening).

    Legacy relies on a blocking recv with no timeout; on a 1 GB VM facing
    the internet, one quiet connection per thread is a cheap DoS. The
    bridge adds an idle timeout the legacy server never needed.
    """

    API_ROUTES = {"/ASN/FI": (200, b"AS1\n")}

    def test_idle_silent_connection_is_reaped_and_service_stays_up(self):
        import time as _time
        FakeAPIHandler.routes = dict(self.API_ROUTES)
        tmpdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmpdir, ignore_errors=True)
        logger = make_query_logger(os.path.join(tmpdir, "q.log"))
        self.server = make_server(("127.0.0.1", 0), self._fake_api, SECRET,
                                  idle_timeout=0.5, querylog=logger)
        self.server_thread = __import__("threading").Thread(
            target=self.server.serve_forever, daemon=True)
        self.server_thread.start()
        self.query = Query(self.server)
        self.addCleanup(self.shutdown)

        idle = socket.create_connection(self.server.server_address, timeout=10)
        idle.sendall(b"")  # connect quietly, send nothing
        deadline = _time.monotonic() + 3.0
        logged = []
        while _time.monotonic() < deadline:
            try:
                with open(os.path.join(tmpdir, "q.log")) as fh:
                    logged = fh.read().strip().splitlines()
            except FileNotFoundError:
                pass
            if any(line.endswith("INVALID") and " 127.0.0.1 - - " in line
                   for line in logged):
                break
            _time.sleep(0.1)
        idle.close()

        self.assertTrue(
            any(" 127.0.0.1 - - INVALID" in line for line in logged),
            msg=f"idle connection was never reaped: {logged}")
        # The service still answers other clients meanwhile.
        self.assertEqual(self.query(b"fi\r\n"), b"AS1\n")


if __name__ == "__main__":
    unittest.main()
