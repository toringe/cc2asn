import unittest
from unittest.mock import MagicMock

from botocore.exceptions import ClientError  # noqa: F401  (re-exported error type in tests)

from lib.s3data import DataStore, historic_key


def make_client(body_text="AS224\nAS2116\n"):
    client = MagicMock()
    body = MagicMock()
    body.read.return_value = body_text.encode("utf-8")
    body.__exit__ = MagicMock(return_value=False)
    response = {"Body": body}
    client.get_object.return_value = response
    client.body = body  # convenience handle for assertions
    return client


class TestKeys(unittest.TestCase):
    def test_latest_key(self):
        store = DataStore("bkt", client=MagicMock())
        store.get_latest("ASN", "NO")
        store.client.get_object.assert_called_once_with(
            Bucket="bkt", Key="latest/NO_ASN"
        )

    def test_historic_key_exact(self):
        store = DataStore("bkt", client=MagicMock())
        store.get_historic("2026-10-27", "IPV4", "US")
        store.client.get_object.assert_called_once_with(
            Bucket="bkt", Key="2026/10/27/US_IPV4"
        )

    def test_historic_key_zero_padded(self):
        self.assertEqual(historic_key("2026-03-05", "ASN", "SE"), "2026/03/05/SE_ASN")


class TestLatestCache(unittest.TestCase):
    def test_second_call_within_ttl_does_not_refetch(self):
        client = make_client()
        store = DataStore("bkt", cache_ttl=300, client=client)
        store.get_latest("ASN", "NO")
        store.get_latest("ASN", "NO")
        self.assertEqual(client.get_object.call_count, 1)

    def test_expired_ttl_refetches(self):
        client = make_client()
        store = DataStore("bkt", cache_ttl=300, client=client)
        clock = {"t": 1000.0}
        store._now = lambda: clock["t"]
        store.get_latest("ASN", "NO")
        clock["t"] = 1301.0
        store.get_latest("ASN", "NO")
        self.assertEqual(client.get_object.call_count, 2)


class TestHistoricNoCache(unittest.TestCase):
    def test_two_calls_hit_s3_twice(self):
        client = make_client()
        store = DataStore("bkt", client=client)
        store.get_historic("2026-10-27", "IPV4", "US")
        store.get_historic("2026-10-27", "IPV4", "US")
        self.assertEqual(client.get_object.call_count, 2)


class TestMissingObject(unittest.TestCase):
    def test_nosuchkey_returns_none(self):
        client = make_client()

        def raise_nosuchkey(**kwargs):
            raise client.exceptions.NoSuchKey({}, "NoSuchKey")

        client.exceptions = MagicMock()
        client.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})
        client.get_object.side_effect = raise_nosuchkey
        store = DataStore("bkt", client=client)
        self.assertIsNone(store.get_latest("ASN", "NO"))

    def test_other_client_error_propagates(self):
        client = make_client()
        client.exceptions = MagicMock()
        client.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})  # typed catch must keep working
        other = type("SomeOtherError", (Exception,), {})
        client.get_object.side_effect = other({}, "boom")
        store = DataStore("bkt", client=client)
        with self.assertRaises(other):
            store.get_latest("ASN", "NO")

    def test_body_is_closed_after_read(self):
        client = make_client()
        store = DataStore("bkt", client=client)
        store.get_latest("ASN", "NO")
        self.assertTrue(client.body.__exit__.called)


if __name__ == "__main__":
    unittest.main()
