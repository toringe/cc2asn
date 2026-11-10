import datetime
import http.client
import json
import threading
import unittest
from unittest.mock import patch

from lib.config import Config
from server import HISTORIC_GONE, Deps, create_server, seconds_until_utc_midnight


TODAY = "2026-10-30"


class StubStore:
    """DataStore stub over a plain dict."""

    def __init__(self, data=None):
        self.data = data or {}
        self.last_source = None

    def get_latest(self, rectype, cc):
        self.last_source = "s3"
        return self.data.get(f"latest/{cc}_{rectype}")


class ForgivingLimiter:
    def __init__(self):
        self.calls = 0
        self.last_ip = None

    def allow(self, ip, today):
        self.calls += 1
        self.last_ip = ip
        return self.calls <= 100

    def daily_exhausted(self, ip, today):
        return False


class ExhaustLimiter:
    """In-memory limiter refusing on the per-minute window."""

    def allow(self, ip, today):
        return False

    def daily_exhausted(self, ip, today):
        return False


class DayExhaustLimiter(ExhaustLimiter):
    """In-memory limiter refusing because its daily counter is used up."""

    def daily_exhausted(self, ip, today):
        return True


class StubDaily:
    def __init__(self, allowed=True):
        self.allowed = allowed
        self.calls = []

    def allow(self, ip, today):
        self.calls.append((ip, today))
        return self.allowed


class ServerFixture(unittest.TestCase):
    def get(self, deps, path, headers=None):
        server = create_server(deps)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = server.server_address[1]
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            conn.request("GET", path, headers=headers or {})
            response = conn.getresponse()
            result = (response.status, dict(response.getheaders()), response.read().decode("utf-8"))
            conn.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
        return result


class RecordingLatestLog:
    def __init__(self, *a):
        self.lines = []

    def write(self, line, stream_date, now_ms=None):
        self.lines.append(line)
        return True


class BrokenLogger:
    def write(self, *a):
        raise RuntimeError("ddb down")


DOCS = {"latest/NO_ASN": "AS224\nAS2116", "2020/01/02/US_ASN": "AS100"}


def good_deps(latest_log=None, daily=None, limiter=None):
    return Deps(
        config=Config(),
        store=StubStore(data=DOCS),
        limiter=limiter or ForgivingLimiter(),
        daily=daily or StubDaily(),
        latest_log=latest_log or RecordingLatestLog(),
    )


class TestServerRouting(ServerFixture):
    def setUp(self):
        self.deps = good_deps()

    def test_banner(self):
        status, _, body = self.get(self.deps, "/")
        self.assertEqual(status, 200)
        self.assertIn("CC2ASN WEB API", body)

    def test_lowercase_country_code(self):
        status_lower, _, body_lower = self.get(self.deps, "/no")
        status_upper, _, body_upper = self.get(self.deps, "/NO")
        self.assertEqual(status_lower, 200)
        self.assertEqual(status_upper, 200)
        self.assertEqual(body_lower, body_upper)
        self.assertEqual(body_lower, "AS224\nAS2116\n")

    def test_json_format(self):
        status, headers, body = self.get(self.deps, "/NO?format=json")
        self.assertEqual(status, 200)
        self.assertEqual(headers.get("Content-Type"), "application/json")
        payload = json.loads(body)
        self.assertEqual(payload["cc"], "NO")
        self.assertEqual(payload["records"], ["AS224", "AS2116"])

    def test_latest_missing_returns_404(self):
        # Unknown-format paths and valid-format-but-absent data both read
        # "Not found"; unknown CCs must NOT surface as 500 Internal error
        # (see the AccessDenied-on-missing-key quirk in lib/s3data.py).
        status, _, body = self.get(good_deps(), "/US")
        self.assertEqual(status, 404)
        self.assertEqual(body, "Not found: /US\n")

    def test_data_date_header_latest(self):
        _, headers, _ = self.get(self.deps, "/NO")
        self.assertEqual(headers.get("X-Data-Date"), "latest")

    def test_banner_states_daily_limit_and_no_historic(self):
        _, _, body = self.get(self.deps, "/")
        self.assertIn("100 queries per day", body)
        self.assertNotIn("YYYY", body)

    def test_historic_paths_not_available(self):
        # With or without the old Bearer token: a clear 404, nothing counted,
        # nothing fetched, nothing logged.
        for path, headers in (
            ("/2020-01-02/US", {}),
            ("/2020-01-02/IPV4/US", {}),
            ("/2020-01-02/US", {"Authorization": "Bearer good"}),
            ("/2020-01-02/US?format=json", {}),
        ):
            with self.subTest(path=path, headers=headers):
                deps = good_deps()
                status, _, body = self.get(deps, path, headers=headers)
                self.assertEqual(status, 404)
                self.assertEqual(body, HISTORIC_GONE)
                self.assertEqual(deps.limiter.calls, 0)
                self.assertEqual(deps.daily.calls, [])
                self.assertEqual(deps.latest_log.lines, [])

    def test_daily_limit_returns_429_until_utc_midnight(self):
        now = datetime.datetime(2026, 11, 10, 23, 0, 0, tzinfo=datetime.timezone.utc)
        with patch("server.utcnow", return_value=now):
            status, headers, body = self.get(good_deps(daily=StubDaily(allowed=False)), "/NO")
        self.assertEqual(status, 429)
        self.assertEqual(body, "Daily limit exceeded (100 queries per day). Resets at 00:00 UTC.\n")
        self.assertEqual(headers.get("Retry-After"), "3600")

    def test_in_memory_day_cap_reports_daily_limit(self):
        # DynamoDB failing open leaves the in-memory day cap; it must answer
        # with the same daily message, not a misleading 60-second retry.
        deps = good_deps(limiter=DayExhaustLimiter())
        status, headers, body = self.get(deps, "/NO")
        self.assertEqual(status, 429)
        self.assertTrue(body.startswith("Daily limit exceeded"))
        self.assertNotEqual(headers.get("Retry-After"), "60")
        self.assertEqual(deps.daily.calls, [])

    def test_daily_limit_uses_client_ip_and_utc_day(self):
        now = datetime.datetime(2026, 11, 10, 23, 30, 0, tzinfo=datetime.timezone.utc)
        deps = good_deps()
        with patch("server.utcnow", return_value=now):
            self.get(deps, "/NO", headers={"X-Forwarded-For": "192.0.2.7"})
        self.assertEqual(deps.daily.calls, [("192.0.2.7", "2026-11-10")])

    def test_burst_limited_requests_skip_the_daily_store(self):
        deps = good_deps(limiter=ExhaustLimiter())
        self.get(deps, "/NO")
        self.assertEqual(deps.daily.calls, [])

    def test_seconds_until_utc_midnight(self):
        utc = datetime.timezone.utc
        self.assertEqual(seconds_until_utc_midnight(datetime.datetime(2026, 11, 10, 23, 59, 30, tzinfo=utc)), 30)
        self.assertEqual(seconds_until_utc_midnight(datetime.datetime(2026, 11, 10, 0, 0, 0, tzinfo=utc)), 86400)

    def test_rate_limit_returns_429_with_retry_header(self):
        deps = Deps(
            config=Config(),
            store=StubStore(data=DOCS),
            limiter=ExhaustLimiter(),
        )
        status, headers, body = self.get(deps, "/NO")
        self.assertEqual(status, 429)
        self.assertEqual(body, "Rate limit exceeded\n")
        self.assertEqual(headers.get("Retry-After"), "60")

    def test_broken_logger_does_not_break_responses(self):
        deps = good_deps(latest_log=BrokenLogger())
        status, _, body = self.get(deps, "/NO")
        self.assertEqual(status, 200)
        self.assertEqual(body, "AS224\nAS2116\n")

    def test_various_signatures_backward_compat(self):
        self.get(self.deps, "/NO")
        status, _, _ = self.get(self.deps, "/US")
        self.assertEqual(status, 404)

    def test_latest_query_logged_in_legacy_format(self):
        import re

        deps = good_deps()
        self.get(deps, "/NO", headers={"X-Forwarded-For": "8.8.8.8"})
        self.assertEqual(len(deps.latest_log.lines), 1)
        pattern = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} 8\.8\.8\.8 ASN NO src=(http|tcp43)$"
        self.assertRegex(deps.latest_log.lines[0], pattern)

    def test_failed_lookups_not_logged_as_queries(self):
        for path in ("/2020-01-02/US", "/US", "/nation"):
            with self.subTest(path=path):
                deps = good_deps()
                self.get(deps, path)
                self.assertEqual(deps.latest_log.lines, [])

    def test_rate_limit_prefers_request_context_IP(self):
        deps = good_deps()
        self.get(
            deps,
            "/NO",
            headers={
                "X-Forwarded-For": "6.6.6.6",
                "x-amzn-request-context": '{"http":{"sourceIp":"2.2.2.2"}}',
            },
        )
        self.assertEqual(deps.limiter.last_ip, "2.2.2.2")

    def test_client_ip_falls_back_to_xff_when_context_unparseable(self):
        deps = good_deps()
        self.get(
            deps,
            "/NO",
            headers={"x-amzn-request-context": "not-json", "X-Forwarded-For": "9.9.9.9"},
        )
        self.assertEqual(deps.limiter.last_ip, "9.9.9.9")

    def test_rate_limit_uses_forwarded_ip(self):
        deps = good_deps()
        self.get(deps, "/NO", headers={"X-Forwarded-For": "8.8.8.8, 10.0.0.1"})
        self.assertEqual(deps.limiter.last_ip, "8.8.8.8")
        self.get(deps, "/NO")
        self.assertNotEqual(deps.limiter.last_ip, None)

    def test_internal_error_returns_500(self):
        class BrokenStore:
            def get_latest(self, rectype, cc):
                raise RuntimeError("boom")

        deps = Deps(
            config=Config(),
            store=BrokenStore(),
            limiter=ForgivingLimiter(),
        )
        status, _, body = self.get(deps, "/NO")
        self.assertEqual(status, 500)
        self.assertEqual(body, "Internal error\n")

    def test_malformed_date_returns_404(self):
        for path in ("/2026-13-01/US", "/2020-01-02/UNKNOWN!", "/nation"):
            with self.subTest(path=path):
                status, _, body = self.get(self.deps, path)
                self.assertEqual(status, 404)



if __name__ == "__main__":
    unittest.main()
