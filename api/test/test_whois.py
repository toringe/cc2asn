import json
import unittest

from lib.whois import Query, parse_query, render_json, render_plain


class TestParseQuery(unittest.TestCase):
    def test_bare_cc_defaults_to_asn(self):
        self.assertEqual(parse_query("/NO"), Query(cc="NO", rectype="ASN", date=None))

    def test_lowercase_cc_accepted(self):
        self.assertEqual(parse_query("/no"), Query(cc="NO", rectype="ASN", date=None))

    def test_explicit_types(self):
        for rectype in ("ALL", "IPV4", "IPV6"):
            with self.subTest(rectype=rectype):
                expected = Query(cc="US", rectype=rectype, date=None)
                self.assertEqual(parse_query(f"/{rectype}/US"), expected)

    def test_unknown_type_rejected(self):
        self.assertIsNone(parse_query("/IPV5/US"))

    def test_three_segment_non_date_paths_rejected(self):
        self.assertIsNone(parse_query("/IPV4/NO/US"))
        self.assertIsNone(parse_query("/downloads/for/NO"))

    def test_overlong_date_paths_rejected(self):
        self.assertIsNone(parse_query("/2020-01-02/ASN/NO/US"))

    def test_empty_and_overlong_paths_rejected(self):
        self.assertIsNone(parse_query("//US"))
        self.assertIsNone(parse_query("/NO/extra"))
        self.assertIsNone(parse_query("/NO/"))
        self.assertIsNone(parse_query("/"))

    def test_date_query_defaults_to_asn(self):
        self.assertEqual(
            parse_query("/2026-10-27/SE"),
            Query(cc="SE", rectype="ASN", date="2026-10-27"),
        )

    def test_date_query_with_type(self):
        self.assertEqual(
            parse_query("/2026-10-27/ASN/SE"),
            Query(cc="SE", rectype="ASN", date="2026-10-27"),
        )

    def test_invalid_dates_rejected(self):
        for date in ("2026-13-01", "2026-02-30", "1234-56-78", "19-01-01"):
            with self.subTest(date=date):
                self.assertIsNone(parse_query(f"/{date}/US"))

    def test_query_string_stripped(self):
        self.assertEqual(parse_query("/NO?format=json"), Query(cc="NO", rectype="ASN", date=None))


class TestRenderPlain(unittest.TestCase):
    def test_adds_single_trailing_newline(self):
        self.assertEqual(render_plain("AS224\nAS2116"), "AS224\nAS2116\n")

    def test_idempotent(self):
        self.assertEqual(render_plain("AS224\n"), "AS224\n")


class TestRenderJson(unittest.TestCase):
    def test_output_is_json_with_records(self):
        query = Query(cc="NO", rectype="ASN", date="2026-10-27")
        payload = json.loads(render_json(query, "2026-10-27", "AS224\nAS2116\n\n"))
        self.assertEqual(payload["cc"], "NO")
        self.assertEqual(payload["type"], "ASN")
        self.assertEqual(payload["date"], "2026-10-27")
        self.assertEqual(payload["records"], ["AS224", "AS2116"])


if __name__ == "__main__":
    unittest.main()
