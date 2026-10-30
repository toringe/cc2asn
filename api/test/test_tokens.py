import hashlib
import unittest
from unittest.mock import MagicMock

from lib.tokens import TokenStore, hash_token


TOKEN = "a" * 32
TODAY = "2026-10-30"


def make_item(**overrides):
    item = {
        "tk": hash_token(TOKEN),
        "name": "test-customer",
        "tier": "historic",
        "daily_quota": 10,
    }
    item.update(overrides)
    return item


def make_table(item=make_item(), raise_on_update=None, second_call_raises=False):
    table = MagicMock()
    table.get_item.return_value = {"Item": item} if item is not None else {}
    table.meta = MagicMock()
    table.meta.client = MagicMock()
    table.meta.client.exceptions.ConditionalCheckFailedException = type(
        "ConditionalCheckFailedException", (Exception,), {}
    )
    if raise_on_update is not None:
        table.update_item.side_effect = raise_on_update
    elif second_call_raises:
        calls = []
        table.update_item.side_effect = lambda **kwargs: (
            calls.append(kwargs),
            (_ for _ in ()).throw(
                table.meta.client.exceptions.ConditionalCheckFailedException({}, "x")
            )
            if len(calls) == 1
            else None,
        )
    return table


class TestAuthorize(unittest.TestCase):
    def test_valid_token_returns_item(self):
        table = make_table()
        store = TokenStore(table)
        result = store.authorize(TOKEN, TODAY)
        self.assertEqual(result, make_item())
        table.get_item.assert_called_once_with(Key={"tk": hash_token(TOKEN)})

    def test_unknown_token_returns_none(self):
        table = make_table(item=None)
        table.get_item.return_value = {}
        store = TokenStore(table)
        self.assertIsNone(store.authorize(TOKEN, TODAY))

    def test_expired_token_returns_none(self):
        table = make_table(item=make_item(expires_on="2026-10-29"))
        store = TokenStore(table)
        self.assertIsNone(store.authorize(TOKEN, TODAY))

    def test_expires_on_today_or_future_is_valid(self):
        for day in (TODAY, "2026-12-31"):
            with self.subTest(expires_on=day):
                table = make_table(item=make_item(expires_on=day))
                store = TokenStore(table)
                self.assertEqual(store.authorize(TOKEN, TODAY), make_item(expires_on=day))


class TestConsume(unittest.TestCase):
    def test_first_use_of_a_day_succeeds(self):
        table = make_table(item=make_item())
        store = TokenStore(table)
        self.assertTrue(store.consume(TOKEN, TODAY))
        call = table.update_item.call_args
        self.assertEqual(call.kwargs["Key"], {"tk": hash_token(TOKEN)})
        self.assertIn("SET", call.kwargs["UpdateExpression"])
        self.assertEqual(call.kwargs["ExpressionAttributeValues"][":today"], TODAY)

    def test_exhausted_day_returns_false(self):
        table = make_table()
        table.update_item.side_effect = (
            table.meta.client.exceptions.ConditionalCheckFailedException({}, "x")
        )
        store = TokenStore(table)
        self.assertFalse(store.consume(TOKEN, TODAY))

    def test_same_day_under_quota_succeeds_with_add(self):
        table = make_table()
        table.update_item.side_effect = [
            table.meta.client.exceptions.ConditionalCheckFailedException({}, "x"),
            None,
        ]
        store = TokenStore(table)
        self.assertTrue(store.consume(TOKEN, TODAY))
        second = table.update_item.call_args_list[1]
        self.assertIn("ADD", second.kwargs["UpdateExpression"])
        self.assertEqual(
            second.kwargs["ExpressionAttributeValues"][":quota"], 10
        )

    def test_non_conditional_ddb_errors_propagate(self):
        table = make_table()
        class ThroughputError(Exception):
            pass
        table.update_item.side_effect = [
            table.meta.client.exceptions.ConditionalCheckFailedException({}, "x"),
            ThroughputError("ProvisionedThroughputExceeded"),
        ]
        store = TokenStore(table)
        with self.assertRaises(ThroughputError):
            store.consume(TOKEN, TODAY)

    def test_day_rollover_resets_counter(self):
        table = make_table(item=make_item(used_date="2026-10-29", used_today=777))
        store = TokenStore(table)
        self.assertTrue(store.consume(TOKEN, TODAY))
        first = table.update_item.call_args_list[0]
        self.assertEqual(first.kwargs["ExpressionAttributeValues"][":today"], TODAY)

    def test_hash_token_is_sha256_hex(self):
        self.assertEqual(hash_token(TOKEN), hashlib.sha256(TOKEN.encode()).hexdigest())


if __name__ == "__main__":
    unittest.main()
