import os
import unittest

from lib.config import load


class TestConfig(unittest.TestCase):
    def test_defaults_with_empty_env(self):
        config = load({})
        self.assertEqual(config.port, 8080)
        self.assertEqual(config.bucket, "cc2asn-db")
        self.assertEqual(config.table, "cc2asn-tokens")
        self.assertEqual(config.rate_per_minute, 30)
        self.assertEqual(config.rate_per_day, 100)
        self.assertEqual(config.cache_ttl, 300)

    def test_overrides_are_read(self):
        env = {
            "PORT": "9000",
            "CC2ASN_DB_BUCKET": "test-bucket",
            "CC2ASN_TOKENS_TABLE": "test-table",
            "RATE_PER_MINUTE": "10",
            "RATE_PER_DAY": "50",
            "CACHE_TTL": "60",
        }
        config = load(env)
        self.assertEqual(config.port, 9000)
        self.assertEqual(config.bucket, "test-bucket")
        self.assertEqual(config.table, "test-table")
        self.assertEqual(config.rate_per_minute, 10)
        self.assertEqual(config.rate_per_day, 50)
        self.assertEqual(config.cache_ttl, 60)

    def test_invalid_integer_raises_value_error(self):
        with self.assertRaises(ValueError):
            load({"RATE_PER_MINUTE": "abc"})

    def test_unknown_env_keys_ignored(self):
        env = dict(os.environ)
        env["TOTALLY_UNRELATED"] = "yes"
        config = load(dict(env, CC2ASN_DB_BUCKET="x"))
        self.assertEqual(config.bucket, "x")


if __name__ == "__main__":
    unittest.main()
