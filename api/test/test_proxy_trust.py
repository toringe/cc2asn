"""CloudFront-fronted requests: trusted proxy IP forwarding.

Contract with the CloudFront distribution: the viewer is the real client;
CloudFront appends `X-Forwarded-For: ..., <viewer ip>` and a fixed origin
header `X-CC2ASN-Proxy: <CC2ASN_PROXY_SECRET>` (set in the distribution
config, never visible to clients). The API trusts the last XFF entry only
when the proxy secret matches (constant-time compare) — direct public
callers hitting the Function URL can never win it. Attribution semantics
match the bridge trust: same `_client_ip()` path, but web traffic stays
`src=http` (the tcp43 marker remains bridge-only).
"""

import json
import unittest

from server import Deps, create_server
from lib.config import Config
from lib.ratelimit import RateLimiter

from test.test_server import StubStore, RecordingLatestLog
from test.test_forwarding import ForwardingTest

PROXY = "pr0xy-s3cret"
DOCS = {"latest/NO_ASN": "AS224"}


def deps(proxy=PROXY, latest_log=None, limiter=None):
    return Deps(
        config=Config(bridge_secret="s3kr3t", proxy_secret=proxy),
        store=StubStore(data=DOCS),
        limiter=limiter or RateLimiter(per_minute=1000, per_day=100000),
        latest_log=latest_log or RecordingLatestLog(),
    )


class ProxyTrustTest(ForwardingTest):
    """Reuse the ForwardingTest helpers (get, logged_ip_and_src)."""

    CONTEXT = {"http": {"sourceIp": "198.51.100.50"}}  # adapter payload

    def test_proxy_request_logs_last_xff_entry_as_http(self):
        log = RecordingLatestLog()
        d = deps(latest_log=log)
        status, _ = self.get(d, "/NO", headers={
            "X-CC2ASN-Proxy": PROXY,
            "X-Forwarded-For": "203.0.113.30, 198.51.100.7",  # CF appends last
            "x-amzn-request-context": json.dumps(self.CONTEXT),
        })
        self.assertEqual(status, 200)
        ip, src = self.logged_ip_and_src(log)
        self.assertEqual(ip, "198.51.100.7")  # viewer IP = last XFF entry
        self.assertEqual(src, "src=http")     # web traffic marker stays http

    def test_proxy_wrong_secret_is_ignored(self):
        log = RecordingLatestLog()
        d = deps(latest_log=log)
        self.get(d, "/NO", headers={
            "X-CC2ASN-Proxy": "not-the-proxy-secret",
            "X-Forwarded-For": "203.0.113.31",
            "x-amzn-request-context": json.dumps(self.CONTEXT),
        })
        ip, _ = self.logged_ip_and_src(log)
        self.assertEqual(ip, "198.51.100.50")

    def test_proxy_no_secret_configured_never_trusts_xff(self):
        log = RecordingLatestLog()
        d = deps(proxy="", latest_log=log)
        self.get(d, "/NO", headers={
            "X-CC2ASN-Proxy": PROXY,
            "X-Forwarded-For": "203.0.113.32",
            "x-amzn-request-context": json.dumps(self.CONTEXT),
        })
        ip, _ = self.logged_ip_and_src(log)
        self.assertEqual(ip, "198.51.100.50")

    def test_proxy_accept_list_of_secrets_and_blank_entries(self):
        log = RecordingLatestLog()
        d = deps(proxy=" old-proxy ,,, new-proxy ", latest_log=log)
        for secret in ("old-proxy", "new-proxy"):
            status, _ = self.get(d, "/NO", headers={
                "X-CC2ASN-Proxy": secret,
                "X-Forwarded-For": "203.0.113.33",
                "x-amzn-request-context": json.dumps(self.CONTEXT),
            })
            self.assertEqual(status, 200, msg=secret)
            ip, src = self.logged_ip_and_src(log)
            self.assertEqual(ip, "203.0.113.33")
            self.assertEqual(src, "src=http")

    def test_proxy_non_ascii_junk_is_ignored_not_500(self):
        log = RecordingLatestLog()
        d = deps(latest_log=log)
        status, _ = self.get(d, "/NO", headers={
            "X-CC2ASN-Proxy": "junk caf\xe9",
            "X-Forwarded-For": "203.0.113.34",
            "x-amzn-request-context": json.dumps(self.CONTEXT),
        })
        self.assertEqual(status, 200)
        ip, _ = self.logged_ip_and_src(log)
        self.assertEqual(ip, "198.51.100.50")

    def test_bridge_secret_is_not_a_proxy_secret(self):
        # Distinct trust namespaces: the bridge secret lives in
        # Authorization; the proxy secret in X-CC2ASN-Proxy. One must
        # never satisfy the other.
        log = RecordingLatestLog()
        d = deps(latest_log=log)  # proxy secret != bridge secret ("s3kr3t")
        self.get(d, "/NO", headers={
            "X-CC2ASN-Proxy": "s3kr3t",
            "X-Forwarded-For": "203.0.113.35",
            "x-amzn-request-context": json.dumps(self.CONTEXT),
        })
        ip, src = self.logged_ip_and_src(log)
        self.assertEqual(ip, "198.51.100.50")
        self.assertEqual(src, "src=http")

    def test_proxied_users_rate_limit_separately(self):
        d = deps(limiter=RateLimiter(per_minute=1, per_day=1000))
        proxy = {"X-CC2ASN-Proxy": PROXY}
        a1, _ = self.get(d, "/NO", headers=proxy | {"X-Forwarded-For": "198.51.100.1"})
        b1, _ = self.get(d, "/NO", headers=proxy | {"X-Forwarded-For": "198.51.100.2"})
        a2, _ = self.get(d, "/NO", headers=proxy | {"X-Forwarded-For": "198.51.100.1"})
        self.assertEqual((a1, b1, a2), (200, 200, 429))


class ConfigProxyTest(unittest.TestCase):
    def test_proxy_secret_env_var(self):
        config = Config()
        self.assertEqual(config.proxy_secret, "")
        from lib.config import load
        config = Config(bridge_secret="s")
        self.assertEqual(config.proxy_secret, "")
        loaded = load({"CC2ASN_PROXY_SECRET": "p"})
        self.assertEqual(loaded.proxy_secret, "p")


if __name__ == "__main__":
    unittest.main()
