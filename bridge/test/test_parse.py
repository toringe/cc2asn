"""Tests for whois query parsing (bridge/server.py: parse_query).

Parity with legacy/cc2asn-server.py: an ISO-3166-1 alpha-2 code alone means
ASN; an optional record type (ALL|ASN|IPV4|IPV6) may precede the code.
"""

import unittest

from server import parse_query


class ParseQueryTest(unittest.TestCase):
    def test_bare_country_defaults_to_asn(self):
        self.assertEqual(parse_query("fi"), ("ASN", "FI"))

    def test_ipv4_type_selects_prefixes(self):
        self.assertEqual(parse_query("ipv4 dk"), ("IPV4", "DK"))

    def test_all_types(self):
        for rectype in ("ALL", "ASN", "IPV4", "IPV6"):
            self.assertEqual(parse_query(f"{rectype.lower()} no"), (rectype, "NO"))

    def test_strips_outer_whitespace_and_newlines(self):
        self.assertEqual(parse_query("  fi\r\n"), ("ASN", "FI"))
        self.assertEqual(parse_query("  ipv4 se "), ("IPV4", "SE"))

    def test_newline_inside_query_rejected_like_legacy(self):
        # legacy strips only the outside; "ipv4\n se" does not match its regex
        self.assertIsNone(parse_query("\tipv4\n se \n"))

    def test_invalid_queries_are_none(self):
        for bad in ("noo", "", "asn", "asn sno", "asn noo", "xx-xx", "4 no",
                    "ipv4", "no1"):
            self.assertIsNone(parse_query(bad), msg=f"expected None for {bad!r}")

    def test_accepts_raw_socket_bytes(self):
        self.assertEqual(parse_query(b"fi"), ("ASN", "FI"))


if __name__ == "__main__":
    unittest.main()
