import datetime
import threading
import unittest
from unittest.mock import MagicMock

from botocore.exceptions import ClientError

from lib.ratelimit import DailyLimit, client_key

TODAY = "2026-11-10"


class FakeTable:
    """In-memory stand-in for the DynamoDB conditional ADD used by DailyLimit."""

    def __init__(self):
        self.items = {}
        self.writes = 0
        self._lock = threading.Lock()
        self.meta = MagicMock()
        self.meta.client.exceptions.ConditionalCheckFailedException = type(
            "ConditionalCheckFailedException", (Exception,), {}
        )

    def update_item(self, Key, UpdateExpression, ConditionExpression,
                    ExpressionAttributeNames, ExpressionAttributeValues):
        with self._lock:
            self.writes += 1
            item = self.items.setdefault(Key["tk"], {})
            count = item.get("n", 0)
            if count >= ExpressionAttributeValues[":limit"]:
                raise self.meta.client.exceptions.ConditionalCheckFailedException()
            item["n"] = count + 1
            item["expires_at"] = ExpressionAttributeValues[":exp"]


def throttled_table():
    table = FakeTable()

    def boom(**kwargs):
        raise ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "x"}},
            "UpdateItem",
        )

    table.update_item = boom
    return table


class TestClientKey(unittest.TestCase):
    def test_ipv4_distinct(self):
        self.assertNotEqual(client_key("192.0.2.1"), client_key("192.0.2.2"))

    def test_ipv6_same_64_shares_a_key(self):
        # One device rotates through privacy addresses inside its /64.
        self.assertEqual(client_key("2001:db8:1:2::1"), client_key("2001:db8:1:2:abcd::9"))

    def test_ipv6_different_64_differs(self):
        self.assertNotEqual(client_key("2001:db8:1:2::1"), client_key("2001:db8:1:3::1"))

    def test_ipv4_mapped_ipv6_equals_ipv4(self):
        self.assertEqual(client_key("::ffff:192.0.2.1"), client_key("192.0.2.1"))

    def test_key_does_not_contain_the_ip(self):
        self.assertNotIn("192.0.2.1", client_key("192.0.2.1"))

    def test_garbage_does_not_raise(self):
        self.assertEqual(client_key("not-an-ip"), client_key("not-an-ip"))


class TestDailyLimit(unittest.TestCase):
    def test_allows_exactly_per_day(self):
        limit = DailyLimit(FakeTable(), per_day=100)
        results = [limit.allow("192.0.2.1", TODAY) for _ in range(101)]
        self.assertEqual(results.count(True), 100)
        self.assertFalse(results[-1])

    def test_clients_counted_separately(self):
        limit = DailyLimit(FakeTable(), per_day=1)
        self.assertTrue(limit.allow("192.0.2.1", TODAY))
        self.assertTrue(limit.allow("192.0.2.2", TODAY))
        self.assertFalse(limit.allow("192.0.2.1", TODAY))

    def test_shared_across_environments(self):
        # Two warm environments (two DailyLimit instances) share one table.
        table = FakeTable()
        a, b = DailyLimit(table, per_day=2), DailyLimit(table, per_day=2)
        self.assertEqual([a.allow("192.0.2.1", TODAY), b.allow("192.0.2.1", TODAY),
                          a.allow("192.0.2.1", TODAY)], [True, True, False])

    def test_new_utc_day_resets(self):
        limit = DailyLimit(FakeTable(), per_day=1)
        self.assertTrue(limit.allow("192.0.2.1", TODAY))
        self.assertFalse(limit.allow("192.0.2.1", TODAY))
        self.assertTrue(limit.allow("192.0.2.1", "2026-11-11"))

    def test_exhausted_client_stops_writing(self):
        # After the first refusal this environment answers from memory, so a
        # client hammering past its limit costs no further DynamoDB writes.
        table = FakeTable()
        limit = DailyLimit(table, per_day=1)
        limit.allow("192.0.2.1", TODAY)
        limit.allow("192.0.2.1", TODAY)
        writes = table.writes
        for _ in range(50):
            self.assertFalse(limit.allow("192.0.2.1", TODAY))
        self.assertEqual(table.writes, writes)

    def test_item_key_and_expiry(self):
        table = FakeTable()
        DailyLimit(table, per_day=5).allow("192.0.2.1", TODAY)
        (key, item), = table.items.items()
        self.assertEqual(key, f"rl#{TODAY}#{client_key('192.0.2.1')}")
        expected = datetime.datetime(2026, 11, 12, tzinfo=datetime.timezone.utc).timestamp()
        self.assertEqual(item["expires_at"], int(expected))

    def test_dynamodb_failure_fails_open(self):
        limit = DailyLimit(throttled_table(), per_day=1)
        self.assertTrue(limit.allow("192.0.2.1", TODAY))
        self.assertTrue(limit.allow("192.0.2.1", TODAY))

    def test_concurrent_requests_never_exceed_limit(self):
        table = FakeTable()
        limits = [DailyLimit(table, per_day=100) for _ in range(4)]
        allowed = []

        def worker(limit):
            for _ in range(50):
                if limit.allow("192.0.2.1", TODAY):
                    allowed.append(1)

        threads = [threading.Thread(target=worker, args=(l,)) for l in limits]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(allowed), 100)


class TestBuildDeps(unittest.TestCase):
    def test_daily_limit_wired_with_config_limit(self):
        from unittest.mock import patch

        from lib.config import Config
        from server import build_deps

        with patch("boto3.resource") as resource:
            deps = build_deps(Config(table="t", rate_per_day=100))
        resource.return_value.Table.assert_called_once_with("t")
        self.assertIsInstance(deps.daily, DailyLimit)
        self.assertEqual(deps.daily.per_day, 100)
        self.assertEqual(deps.limiter.per_day, 100)


if __name__ == "__main__":
    unittest.main()
