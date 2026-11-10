import unittest
from unittest.mock import MagicMock

from botocore.exceptions import ClientError  # noqa: F401  (re-exported error type in tests)

from lib.s3data import DataStore


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

    def test_last_source_reports_cache_and_s3(self):
        client = make_client()
        store = DataStore("bkt", cache_ttl=300, client=client)
        store.get_latest("ASN", "NO")
        self.assertEqual(store.last_source, "s3")
        store.get_latest("ASN", "NO")
        self.assertEqual(store.last_source, "cache")



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


class TestMissingObject(unittest.TestCase):
    def test_nosuchkey_returns_none(self):
        client = make_client()

        def raise_nosuchkey(**kwargs):
            raise client.exceptions.NoSuchKey({}, "NoSuchKey")

        client.exceptions = MagicMock()
        client.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})
        client.exceptions.AccessDenied = type("AccessDenied", (Exception,), {})  # new: typed catch member
        client.get_object.side_effect = raise_nosuchkey
        store = DataStore("bkt", client=client)
        self.assertIsNone(store.get_latest("ASN", "NO"))

    def test_accessdenied_on_missing_key_returns_none(self):
        # S3's REST API answers a GetObject for a nonexistent key with
        # AccessDenied (403) when the caller lacks s3:ListBucket — the
        # read-only role never gets it, so S3 cannot confirm the key
        # exists. Missing data must degrade to None, not raise.
        client = make_client()

        def raise_accessdenied(**kwargs):
            raise client.exceptions.AccessDenied(
                {"Error": {"Code": "AccessDenied", "Message": "access denied"}}, "GetObject")

        client.exceptions = MagicMock()
        client.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})
        client.exceptions.AccessDenied = type("AccessDenied", (Exception,), {})
        client.get_object.side_effect = raise_accessdenied
        store = DataStore("bkt", client=client)
        self.assertIsNone(store.get_latest("ASN", "NV"))

    def test_other_client_error_propagates(self):
        client = make_client()
        client.exceptions = MagicMock()
        client.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})  # typed catch must keep working
        client.exceptions.AccessDenied = type("AccessDenied", (Exception,), {})  # new: typed catch member
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
