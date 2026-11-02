import http.client
import json
import threading
import unittest

from lib.config import Config
from server import Deps, create_server


TODAY = "2026-10-30"


class StubStore:
    """DataStore stub over a plain dict."""

    def __init__(self, data=None):
        self.data = data or {}
        self.last_source = None

    def get_latest(self, rectype, cc):
        self.last_source = "s3"
        return self.data.get(f"latest/{cc}_{rectype}")

    def get_historic(self, date, rectype, cc):
        self.last_source = "s3"
        y, m, d = date.split("-")
        return self.data.get(f"{y}/{m}/{d}/{cc}_{rectype}")


class StubTokens:
    def __init__(self, valid=(), exhausted=()):
        self.valid = {t: {"tk": t, "name": n} for t, n in valid}
        self.exhausted = set(exhausted)

    def authorize(self, token, today):
        return self.valid.get(token)

    def consume(self, token, today):
        return token not in self.exhausted


class ForgivingLimiter:
    def __init__(self):
        self.calls = 0
        self.last_ip = None

    def allow(self, ip, today):
        self.calls += 1
        self.last_ip = ip
        return self.calls <= 100


class ExhaustLimiter:
    def allow(self, ip, today):
        return False


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


class RecordingHistoricLog(RecordingLatestLog):
    pass


class BrokenLogger:
    def write(self, *a):
        raise RuntimeError("ddb down")


DOCS = {"latest/NO_ASN": "AS224\nAS2116", "2020/01/02/US_ASN": "AS100"}


def good_deps(latest_log=None, historic_log=None, tokens=None):
    return Deps(
        config=Config(),
        store=StubStore(data=DOCS),
        tokens=tokens or StubTokens(valid=[("good", "c1")]),
        limiter=ForgivingLimiter(),
        latest_log=latest_log or RecordingLatestLog(),
        historic_log=historic_log or RecordingHistoricLog(),
    )


class TestServerRouting(ServerFixture):
    def setUp(self):
        self.deps = good_deps()

    def test_banner(self):
        status, _, body = self.get(self.deps, "/")
        self.assertEqual(status, 200)
        self.assertIn("CC2ASN WHOIS-WEB API", body)

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
        status, _, body = self.get(good_deps(), "/US")
        self.assertEqual(status, 404)
        self.assertEqual(body, "Missing data: latest/US_ASN\n")

    def test_data_date_header_latest_and_historic(self):
        _, headers, _ = self.get(self.deps, "/NO")
        self.assertEqual(headers.get("X-Data-Date"), "latest")
        _, headers, _ = self.get(
            self.deps, "/2020-01-02/US", headers={"Authorization": "Bearer good"}
        )
        self.assertEqual(headers.get("X-Data-Date"), "2020-01-02")

    def test_www_authenticate_on_unauthorized(self):
        status, headers, body = self.get(self.deps, "/2020-01-02/US")
        self.assertEqual(status, 401)
        self.assertEqual(body, "Unauthorized\n")
        self.assertEqual(headers.get("WWW-Authenticate"), "Bearer")

    def test_historic_with_valid_token(self):
        status, headers, body = self.get(
            self.deps, "/2020-01-02/US", headers={"Authorization": "Bearer good"}
        )
        self.assertEqual(status, 200)
        self.assertEqual(body, "AS100\n")
        self.assertEqual(headers.get("Content-Type"), "text/plain; charset=utf-8")

    def test_bearer_scheme_case_insensitive(self):
        status, _, _ = self.get(
            self.deps, "/2020-01-02/US", headers={"Authorization": "bEaReR good"}
        )
        self.assertEqual(status, 200)

    def test_authorization_variants(self):
        for header in (None, "good", "Bearer bad", "Basic good", "Bearer"):
            with self.subTest(header=header):
                headers = {"Authorization": header} if header else {}
                status, _, body = self.get(self.deps, "/2020-01-02/US", headers=headers)
                self.assertEqual(status, 401)
                self.assertEqual(body, "Unauthorized\n")

    def test_token_quota_exhausted_returns_429(self):
        deps = good_deps(
            tokens=StubTokens(valid=[("good", "c1")], exhausted=["good"])
        )
        status, _, body = self.get(
            deps, "/2020-01-02/US", headers={"Authorization": "Bearer good"}
        )
        self.assertEqual(status, 429)
        self.assertEqual(body, "Token quota exhausted\n")

    def test_rate_limit_returns_429_with_retry_header(self):
        deps = Deps(
            config=Config(),
            store=StubStore(data=DOCS),
            tokens=StubTokens(),
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
        pattern = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} 8\.8\.8\.8 ASN NO$"
        self.assertRegex(deps.latest_log.lines[0], pattern)
        self.assertEqual(deps.historic_log.lines, [])

    def test_historic_query_logged_enriched(self):
        import re

        deps = good_deps()
        status, _, _ = self.get(
            deps, "/2020-01-02/US", headers={"Authorization": "Bearer good"}
        )
        self.assertEqual(status, 200)
        pattern = (
            r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \S+ ASN US "
            r"date=2020-01-02 status=200 ms=\d+ src=s3 tier=\S* tok=good"
        )
        self.assertRegex(deps.historic_log.lines[0], pattern)

    def test_failed_lookups_not_logged_as_queries(self):
        for path, headers in (
            ("/2020-01-02/US", {}),
            ("/US", {}),
            ("/2020-01-03/US", {"Authorization": "Bearer good"}),
        ):
            with self.subTest(path=path):
                deps = good_deps()
                self.get(deps, path, headers=headers)
                self.assertEqual(deps.latest_log.lines, [])
                self.assertEqual(deps.historic_log.lines, [])

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
            tokens=StubTokens(),
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

    def test_missing_date_returns_404(self):
        status, _, body = self.get(
            self.deps, "/2020-01-03/US", headers={"Authorization": "Bearer good"}
        )
        self.assertEqual(status, 404)
        self.assertEqual(body, "Missing data: 2020/01/03/US_ASN\n")


if __name__ == "__main__":
    unittest.main()
