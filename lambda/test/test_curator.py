"""Offline unit tests for the curator's merge/sync logic (no AWS calls).

Run from lambda/:  uv run python -m unittest discover -s test -t .
"""
import hashlib
import unittest

import curator


def etag(body: str) -> str:
    return '"' + hashlib.md5(body.encode("utf-8")).hexdigest() + '"'


ARIN = {
    "US": {"ASN": ["AS7018", "AS701"], "IPV4": ["8.0.0.0/9"], "IPV6": ["2600::/12"]},
    "CA": {"ASN": ["AS577"]},
    "DATE": "20261110",
}
LACNIC = {
    "US": {"ASN": ["AS263724"], "IPV4": ["179.60.192.0/22"]},
    "BR": {"ASN": ["AS28573"]},
    "DATE": "20261110",
}
RIPE = {
    "US": {"ASN": ["AS701"]},  # duplicate of an ARIN record
    "DE": {"IPV4": ["5.0.0.0/16"]},
    "": {"ASN": ["AS1"]},  # allocated row without a country code
    "DATE": "20261109",
}


class TestMerge(unittest.TestCase):
    def test_country_in_several_registries_keeps_all_records(self):
        ccdata, _ = curator.merge_parsed([ARIN, LACNIC, RIPE])
        self.assertEqual(ccdata["US"]["ASN"], ["AS701", "AS7018", "AS263724"])
        self.assertEqual(ccdata["US"]["IPV4"], ["8.0.0.0/9", "179.60.192.0/22"])

    def test_result_does_not_depend_on_registry_order(self):
        a, _ = curator.merge_parsed([ARIN, LACNIC, RIPE])
        b, _ = curator.merge_parsed([RIPE, LACNIC, ARIN])
        self.assertEqual(a, b)

    def test_duplicate_records_are_listed_once(self):
        ccdata, _ = curator.merge_parsed([ARIN, RIPE])
        self.assertEqual(ccdata["US"]["ASN"].count("AS701"), 1)

    def test_newest_generation_date_is_used(self):
        _, date = curator.merge_parsed([RIPE, ARIN])
        self.assertEqual(date, "20261110")

    def test_missing_dates_give_none(self):
        _, date = curator.merge_parsed([{"US": {"ASN": ["AS1"]}}])
        self.assertIsNone(date)

    def test_empty_country_code_is_dropped(self):
        ccdata, _ = curator.merge_parsed([RIPE])
        self.assertNotIn("", ccdata)

    def test_input_dicts_are_not_mutated(self):
        arin = {"US": {"ASN": ["AS1"]}, "DATE": "20261110"}
        curator.merge_parsed([arin])
        self.assertEqual(arin, {"US": {"ASN": ["AS1"]}, "DATE": "20261110"})


class TestBuildFiles(unittest.TestCase):
    def test_type_files_and_all_file(self):
        ccdata, _ = curator.merge_parsed([ARIN, LACNIC])
        files = curator.build_files(ccdata)
        self.assertEqual(files["US_ASN"], "AS701\nAS7018\nAS263724")
        self.assertEqual(
            files["US_ALL"],
            "AS701\nAS7018\nAS263724\n8.0.0.0/9\n179.60.192.0/22\n2600::/12",
        )
        self.assertEqual(files["BR_ASN"], "AS28573")
        self.assertNotIn("BR_IPV4", files)


class TestPlanSync(unittest.TestCase):
    def test_unchanged_files_are_skipped(self):
        files = {"US_ASN": "AS1", "CA_ASN": "AS2"}
        existing = {"US_ASN": etag("AS1"), "CA_ASN": etag("old")}
        puts, deletes = curator.plan_sync(files, existing)
        self.assertEqual(puts, ["CA_ASN"])
        self.assertEqual(deletes, [])

    def test_new_files_are_written(self):
        puts, _ = curator.plan_sync({"US_ASN": "AS1"}, {})
        self.assertEqual(puts, ["US_ASN"])

    def test_stale_files_are_deleted(self):
        files = {"US_ASN": "AS1"}
        existing = {"US_ASN": etag("AS1"), "US_IPV6": etag("x"), "XX_ALL": etag("y")}
        _, deletes = curator.plan_sync(files, existing)
        self.assertEqual(deletes, ["US_IPV6", "XX_ALL"])


if __name__ == "__main__":
    unittest.main()
