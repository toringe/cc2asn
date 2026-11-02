import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from lib.querylog import QueryLog, historic_line, legacy_line

import os


UTC = timezone.utc


class TestLegacyLine(unittest.TestCase):
    def test_matches_whois_format(self):
        ts = datetime(2026, 11, 1, 13, 4, 51, tzinfo=UTC)
        self.assertEqual(
            legacy_line(ts, "141.101.104.69", "IPV4", "US"),
            "2026-11-01 13:04:51 141.101.104.69 IPV4 US",
        )

    def test_naive_timestamp_treated_as_utc(self):
        ts = datetime(2026, 11, 1, 13, 4, 51)
        self.assertEqual(
            legacy_line(ts, "141.101.104.69", "ASN", "NO"),
            "2026-11-01 13:04:51 141.101.104.69 ASN NO",
        )

    def test_aware_tz_converted_to_utc(self):
        ts = datetime(2026, 11, 1, 14, 4, 51, tzinfo=UTC)  # non-UTC input timezone
        from datetime import timedelta
        cet = timezone(timedelta(hours=2))
        ts_cet = datetime(2026, 11, 1, 16, 4, 51, tzinfo=cet)
        self.assertEqual(legacy_line(ts, "x", "ASN", "NO"), legacy_line(ts_cet, "x", "ASN", "NO"))


    def test_src_suffix_appended_when_given(self):
        ts = datetime(2026, 11, 1, 13, 4, 51, tzinfo=UTC)
        self.assertEqual(
            legacy_line(ts, "141.101.104.69", "IPV4", "US", src="http"),
            "2026-11-01 13:04:51 141.101.104.69 IPV4 US src=http",
        )
        self.assertEqual(
            legacy_line(ts, "141.101.104.69", "IPV4", "US", src="tcp43"),
            "2026-11-01 13:04:51 141.101.104.69 IPV4 US src=tcp43",
        )
        self.assertEqual(
            legacy_line(ts, "141.101.104.69", "IPV4", "US"),
            "2026-11-01 13:04:51 141.101.104.69 IPV4 US",
        )


class TestHistoricLine(unittest.TestCase):
    def test_legacy_prefix_plus_enrichment(self):
        ts = datetime(2026, 11, 1, 13, 7, 2, tzinfo=UTC)
        self.assertEqual(
            historic_line(ts, "108.162.215.226", "ASN", "KE",
                          date="2020-03-15", ms=6, source="s3", tier="standard",
                          tok_prefix="c1a9b2c3"),
            "2026-11-01 13:07:02 108.162.215.226 ASN KE "
            "date=2020-03-15 status=200 ms=6 src=s3 tier=standard tok=c1a9b2c3",
        )


class TestQueryLogWrite(unittest.TestCase):
    def make_client(self):
        client = MagicMock()
        client.exceptions = MagicMock()
        client.exceptions.ResourceAlreadyExistsException = type(
            "ResourceAlreadyExistsException", (Exception,), {}
        )
        return client

    def test_first_write_creates_stream_and_puts_event(self):
        client = self.make_client()
        log = QueryLog("/cc2asn/api/latest", client=client)
        log.write("2026-11-01 13:04:51 x ASN US", stream_date="2026/11/01", now_ms=1793574291000)
        client.create_log_stream.assert_called_once_with(
            logGroupName="/cc2asn/api/latest", logStreamName="2026/11/01"
        )
        client.put_log_events.assert_called_once_with(
            logGroupName="/cc2asn/api/latest",
            logStreamName="2026/11/01",
            logEvents=[{"timestamp": 1793574291000, "message": "2026-11-01 13:04:51 x ASN US"}],
        )

    def test_same_day_second_write_skips_stream_creation(self):
        client = self.make_client()
        log = QueryLog("/cc2asn/api/latest", client=client)
        log.write("a", stream_date="2026/11/01", now_ms=1)
        log.write("b", stream_date="2026/11/01", now_ms=2)
        client.create_log_stream.assert_called_once()
        self.assertEqual(client.put_log_events.call_count, 2)

    def test_already_exists_race_tolerated(self):
        client = self.make_client()
        client.create_log_stream.side_effect = client.exceptions.ResourceAlreadyExistsException(
            {}, "x"
        )
        log = QueryLog("/cc2asn/api/latest", client=client)
        log.write("a", stream_date="2026/11/01", now_ms=1)
        self.assertEqual(client.put_log_events.call_count, 1)

    def test_day_rollover_creates_new_stream(self):
        client = self.make_client()
        log = QueryLog("/cc2asn/api/latest", client=client)
        log.write("a", stream_date="2026/11/01", now_ms=1)
        log.write("b", stream_date="2026/11/02", now_ms=2)
        client.create_log_stream.assert_any_call(logGroupName="/cc2asn/api/latest", logStreamName="2026/11/02")
        self.assertEqual(client.create_log_stream.call_count, 2)

    def test_other_errors_propagate(self):
        client = self.make_client()
        client.exceptions.ResourceNotFoundException = type(
            "ResourceNotFoundException", (Exception,), {}
        )
        boom = type("Boom", (Exception,), {})
        client.create_log_stream.side_effect = boom({}, "x")
        log = QueryLog("/cc2asn/api/latest", client=client)
        with self.assertRaises(boom):
            log.write("a", stream_date="2026/11/01", now_ms=1)


class TestLogClientHardening(unittest.TestCase):
    def test_lazy_client_has_bounded_timeouts(self):
        from unittest.mock import patch as mock_patch

        with mock_patch("boto3.client") as client_factory:
            log = QueryLog("/g")
            _ = log.client  # triggers lazy construction
            kwargs = client_factory.call_args.kwargs
            self.assertEqual(kwargs["config"].connect_timeout, 1)
            self.assertEqual(kwargs["config"].read_timeout, 2)
            self.assertEqual(kwargs["config"].retries["max_attempts"], 1)

    def test_missing_log_group_is_recovered(self):
        client = self.make_client()
        client.exceptions.ResourceNotFoundException = type(
            "ResourceNotFoundException", (Exception,), {}
        )
        # First stream creation fails on the missing group; recovery follows.
        client.create_log_stream.side_effect = [
            client.exceptions.ResourceNotFoundException({}, "x"),
            {},
        ]
        log = QueryLog("/cc2asn/api/latest", client=client)
        log.write("a", stream_date="2026/11/01", now_ms=1)
        client.create_log_group.assert_called_once_with(
            logGroupName="/cc2asn/api/latest"
        )
        self.assertEqual(client.put_log_events.call_count, 1)

    def make_client(self):
        client = MagicMock()
        body = MagicMock()
        body.read.return_value = b""
        body.__exit__ = MagicMock(return_value=False)
        client.get_object.return_value = {"Body": body}
        client.exceptions = MagicMock()
        client.exceptions.ResourceAlreadyExistsException = type(
            "ResourceAlreadyExistsException", (Exception,), {}
        )
        client.exceptions.ResourceNotFoundException = type(
            "ResourceNotFoundException", (Exception,), {}
        )
        return client

    def test_other_errors_propagate(self):
        client = self.make_client()
        other = type("SomeOtherError", (Exception,), {})
        client.create_log_stream.side_effect = other({}, "x")
        log = QueryLog("/cc2asn/api/latest", client=client)
        with self.assertRaises(other):
            log.write("a", stream_date="2026/11/01", now_ms=1)


if __name__ == "__main__":
    unittest.main()
