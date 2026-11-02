"""Bridge-originated requests: trusted IP forwarding and src=tcp43 logging.

Contract with bridge/server.py: the bridge sends
    Authorization: Bearer <CC2ASN_BRIDGE_SECRET>
    X-Forwarded-For: <user ip>
The API trusts the forwarded IP only when the secret matches (constant-time
compare) — direct public users can never win it (they could otherwise forge
XFF and reclaim a fresh rate-limit bucket). Every latest-day lookup is tagged
` src=http|tcp43` in the /cc2asn/api/latest line.
"""

import http.client
import json
import threading
import unittest

from server import Deps, create_server
from lib.config import Config
from lib.ratelimit import RateLimiter

from test.test_server import StubStore, RecordingLatestLog

SECRET = "s3kr3t"
DOCS = {"latest/NO_ASN": "AS224\nAS2116"}


def deps(secret=SECRET, latest_log=None, limiter=None):
    return Deps(
        config=Config(bridge_secret=secret),
        store=StubStore(data=DOCS),
        tokens=None,
        limiter=limiter or RateLimiter(per_minute=1000, per_day=100000),
        latest_log=latest_log or RecordingLatestLog(),
        historic_log=None,
    )


class ForwardingTest(unittest.TestCase):
    def get(self, d, path, headers=None):
        server = create_server(d)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = server.server_address[1]
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            conn.request("GET", path, headers=headers or {})
            response = conn.getresponse()
            result = (response.status, response.read().decode("utf-8"))
            conn.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
        return result

    def logged_ip_and_src(self, latest_log):
        """Extract (ip, src) from the single recorded line.

        Line: `<date> <time> <ip> <rectype> <cc> src=<...>`.
        """
        parts = latest_log.lines[0].split()
        return parts[2], parts[-1]

    def test_bridge_request_logs_forwarded_ip_and_tcp43(self):
        log = RecordingLatestLog()
        d = deps(latest_log=log)
        status, _ = self.get(
            d, "/NO",
            headers={"Authorization": "Bearer " + SECRET,
                     "X-Forwarded-For": "203.0.113.9"})
        self.assertEqual(status, 200)
        ip, src = self.logged_ip_and_src(log)
        self.assertEqual(ip, "203.0.113.9")
        self.assertEqual(src, "src=tcp43")

    def test_direct_request_logs_http_src(self):
        log = RecordingLatestLog()
        d = deps(latest_log=log)
        status, _ = self.get(d, "/NO")
        self.assertEqual(status, 200)
        ip, src = self.logged_ip_and_src(log)
        self.assertEqual(ip, "127.0.0.1")  # local dev fallback, no adapter here
        self.assertEqual(src, "src=http")

    CONTEXT = {"http": {"sourceIp": "198.51.100.50"}}  # adapter payload, production-like

    def test_forged_xff_without_secret_is_ignored(self):
        # In production the adapter always supplies x-amzn-request-context;
        # a public caller who forges XFF (with no/wrong secret) resolves to
        # their real IP from the context, never the forged header.
        log = RecordingLatestLog()
        d = deps(latest_log=log)
        self.get(d, "/NO", headers={
            "X-Forwarded-For": "203.0.113.10",
            "x-amzn-request-context": json.dumps(self.CONTEXT),
        })
        ip, _ = self.logged_ip_and_src(log)
        self.assertEqual(ip, "198.51.100.50")

    def test_wrong_secret_is_ignored(self):
        log = RecordingLatestLog()
        d = deps(latest_log=log)
        self.get(d, "/NO", headers={"Authorization": "Bearer not-the-secret",
                                    "X-Forwarded-For": "203.0.113.11",
                                    "x-amzn-request-context": json.dumps(self.CONTEXT)})
        ip, _ = self.logged_ip_and_src(log)
        self.assertEqual(ip, "198.51.100.50")

    def test_no_secret_configured_never_trusts_xff(self):
        log = RecordingLatestLog()
        d = deps(secret="", latest_log=log)
        self.get(d, "/NO", headers={"Authorization": "Bearer " + SECRET,
                                    "X-Forwarded-For": "203.0.113.12",
                                    "x-amzn-request-context": json.dumps(self.CONTEXT)})
        ip, _ = self.logged_ip_and_src(log)
        self.assertEqual(ip, "198.51.100.50")

    def test_accept_list_of_secrets(self):
        # Rotation support: CC2ASN_BRIDGE_SECRET is a comma-separated list;
        # during rotation both the outgoing and the incoming secret are
        # trusted, so no query ever degrades to a shared anonymous bucket.
        log = RecordingLatestLog()
        d = deps(secret="old-secret,new-secret", latest_log=log)
        for secret in ("old-secret", "new-secret"):
            status, _ = self.get(d, "/NO", headers={
                "Authorization": "Bearer " + secret,
                "X-Forwarded-For": "203.0.113.20",
                "x-amzn-request-context": json.dumps(self.CONTEXT),
            })
            self.assertEqual(status, status_ok := 200, msg=secret)
            ip, src = self.logged_ip_and_src(log)
            self.assertEqual(ip, "203.0.113.20")
            self.assertEqual(src, "src=tcp43")

    def test_accept_list_still_rejects_unknown_secret(self):
        log = RecordingLatestLog()
        d = deps(secret="old-secret,new-secret", latest_log=log)
        self.get(d, "/NO", headers={
            "Authorization": "Bearer other",
            "X-Forwarded-For": "203.0.113.21",
            "x-amzn-request-context": json.dumps(self.CONTEXT),
        })
        ip, src = self.logged_ip_and_src(log)
        self.assertEqual(ip, "198.51.100.50")
        self.assertEqual(src, "src=http")

    def test_accept_list_ignores_blank_entries_and_spaces(self):
        log = RecordingLatestLog()
        d = deps(secret=" good-secret ,,, ", latest_log=log)
        self.get(d, "/NO", headers={"Authorization": "Bearer good-secret",
                                    "X-Forwarded-For": "203.0.113.22"})
        ip, src = self.logged_ip_and_src(log)
        self.assertEqual(ip, "203.0.113.22")
        self.assertEqual(src, "src=tcp43")

    def test_non_ascii_authorization_is_ignored_not_500(self):
        # An attacker-proof (and practically occurring, e.g. shell junk)
        # latin-1 header must not crash hmac.compare_digest into a 500.
        # Real behavior requested: treat it like any non-matching secret.
        log = RecordingLatestLog()
        d = deps(latest_log=log)
        status, _ = self.get(d, "/NO", headers={
            "Authorization": "Bearer caf\xe9",  # latin-1 'é'
            "x-amzn-request-context": json.dumps(self.CONTEXT),
        })
        self.assertEqual(status, 200)
        ip, src = self.logged_ip_and_src(log)
        self.assertEqual(ip, "198.51.100.50")
        self.assertEqual(src, "src=http")

    def test_bridge_xff_wins_over_adapter_context(self):
        # The bridge's forwarded user IP beats the adapter context (which
        # would name the bridge VM), even in production.
        log = RecordingLatestLog()
        d = deps(latest_log=log)
        self.get(d, "/NO", headers={"Authorization": "Bearer " + SECRET,
                                    "X-Forwarded-For": "203.0.113.9",
                                    "x-amzn-request-context": json.dumps(self.CONTEXT)})
        ip, src = self.logged_ip_and_src(log)
        self.assertEqual(ip, "203.0.113.9")
        self.assertEqual(src, "src=tcp43")

    def test_bridged_users_rate_limit_separately(self):
        # per_minute=1: each forwarded user gets their own bucket.
        d = deps(limiter=RateLimiter(per_minute=1, per_day=1000))
        bridge = {"Authorization": "Bearer " + SECRET}
        status_a1, _ = self.get(d, "/NO", headers=bridge | {"X-Forwarded-For": "198.51.100.1"})
        status_b1, _ = self.get(d, "/NO", headers=bridge | {"X-Forwarded-For": "198.51.100.2"})
        status_a2, _ = self.get(d, "/NO", headers=bridge | {"X-Forwarded-For": "198.51.100.1"})
        self.assertEqual((status_a1, status_b1, status_a2), (200, 200, 429))


class ConfigTest(unittest.TestCase):
    def test_bridge_secret_env_var(self):
        config = Config()
        self.assertEqual(config.bridge_secret, "")
        from lib.config import load
        config = load({"CC2ASN_BRIDGE_SECRET": "tok"})
        self.assertEqual(config.bridge_secret, "tok")


if __name__ == "__main__":
    unittest.main()
