"""Tests for the bridge API client (bridge/lib/api.py).

Runs with the in-process fake API server; asserts the exact HTTP contract
the deployed CC2ASN-api expects from the bridge: latest-day lookups over
`GET /{TYPE}/{CC}` with the bridge secret and the originating user's IP.
"""

import http.server
import threading
import unittest

from lib.api import get


SECRET = "test-secret-123"


class FakeAPIHandler(http.server.BaseHTTPRequestHandler):
    """Serves configured (status, body) per exact path; records requests."""

    routes = {}  # path -> (status, body)
    last_headers = None
    last_path = None
    quiet = True

    def do_GET(self):
        type(self).last_headers = dict(self.headers)
        type(self).last_path = self.path
        status, body = type(self).routes.get(self.path, (404, b"no such file"))
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        if not self.quiet:
            super().log_message(format, *args)


class FakeAPI:
    """Boots FakeAPIHandler on an ephemeral port; attribute base_url."""

    def __enter__(self):
        self.server = http.server.HTTPServer(("127.0.0.1", 0), FakeAPIHandler)
        self.base_url = "http://127.0.0.1:%d" % self.server.server_port
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


class APIClientTest(unittest.TestCase):
    BASE = "http://base.invalid"  # never used directly in route tests

    def setUp(self):
        FakeAPIHandler.routes = {}
        FakeAPIHandler.last_headers = None
        FakeAPIHandler.last_path = None

    def test_sends_query_to_latest_endpoint(self):
        with FakeAPI() as api:
            FakeAPIHandler.routes["/ASN/FI"] = (200, b"AS1\nAS2\n")
            status, body = get(api.base_url, SECRET, "203.0.113.7", "ASN", "FI")
        self.assertEqual(FakeAPIHandler.last_path, "/ASN/FI")
        self.assertEqual(status, 200)
        self.assertEqual(body, b"AS1\nAS2\n")

    def test_sends_query_with_uppercased_type(self):
        with FakeAPI() as api:
            FakeAPIHandler.routes["/IPV4/DK"] = (200, b"1.2.3.0/24\n")
            status, body = get(api.base_url, SECRET, "203.0.113.7", "ipv4", "dk")
        self.assertEqual(FakeAPIHandler.last_path, "/IPV4/DK")
        self.assertEqual(status, 200)
        self.assertEqual(body, b"1.2.3.0/24\n")

    def test_sends_authorization_bearer_secret(self):
        with FakeAPI() as api:
            FakeAPIHandler.routes["/ASN/NO"] = (200, b"AS1\n")
            get(api.base_url, SECRET, "203.0.113.7", "ASN", "NO")
        self.assertEqual(FakeAPIHandler.last_headers.get("Authorization"),
                         "Bearer " + SECRET)

    def test_sends_client_ip_as_forwarded_for(self):
        with FakeAPI() as api:
            FakeAPIHandler.routes["/ASN/NO"] = (200, b"AS1\n")
            get(api.base_url, SECRET, "203.0.113.7", "ASN", "NO")
        self.assertEqual(FakeAPIHandler.last_headers.get("X-Forwarded-For"),
                         "203.0.113.7")

    def test_maps_404_missing_country(self):
        with FakeAPI() as api:
            status, body = get(api.base_url, SECRET, "203.0.113.7", "ASN", "XX")
        self.assertEqual(status, 404)

    def test_maps_429_rate_limited(self):
        with FakeAPI() as api:
            FakeAPIHandler.routes["/ASN/NO"] = (429, b"limit")
            status, body = get(api.base_url, SECRET, "203.0.113.7", "ASN", "NO")
        self.assertEqual(status, 429)

    def test_maps_connection_failure_to_status_zero(self):
        # base_url points at a port with no server listening
        dead_base = "http://127.0.0.1:1"
        status, body = get(dead_base, SECRET, "203.0.113.7", "ASN", "NO")
        self.assertEqual(status, 0)
        self.assertEqual(body, b"")


if __name__ == "__main__":
    unittest.main()
