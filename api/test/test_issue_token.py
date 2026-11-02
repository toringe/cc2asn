import io
import os
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import MagicMock, patch

sys.path.insert(0, "scripts")

from lib.tokens import hash_token

import issue_token


class TestTableResolution(unittest.TestCase):
    def test_table_name_from_env(self):
        table = MagicMock()
        resource = MagicMock()
        resource.Table = MagicMock(return_value=table)
        table.scan.return_value = {"Items": [{"tk": "hash123", "name": "acme"}]}
        buf = io.StringIO()
        stderr = io.StringIO()
        with patch.object(issue_token.boto3, "resource", return_value=resource):
            with patch.dict(os.environ, {"CC2ASN_TOKENS_TABLE": "custom-table"}):
                with redirect_stdout(buf), redirect_stderr(stderr):
                    code = issue_token.main(["--revoke", "acme"])
        self.assertEqual(code, 0)
        table.scan.assert_called_once()
        table.delete_item.assert_called_once_with(Key={"tk": "hash123"})
        resource.Table.assert_called_once_with("custom-table")
        resource.Table.assert_called_once_with("custom-table")


class TestIssueToken(unittest.TestCase):
    def _run(self, argv, table):
        with patch.object(issue_token.boto3, "resource", return_value=MagicMock()) as resource:
            with patch("sys.stderr", new=io.StringIO()):
                resource.return_value.Table.return_value = table
                buf = io.StringIO()
                with redirect_stdout(buf):
                    code = issue_token.main(argv)
                return code, buf.getvalue()

    def test_create_prints_token_and_puts_hashed_item(self):
        table = MagicMock()
        code, out = self._run(["--name", "acme", "--quota", "500", "--days", "30"], table)
        self.assertEqual(code, 0)
        token = out.split()[0]
        self.assertEqual(len(token), 32)
        args = table.put_item.call_args.kwargs["Item"]
        self.assertEqual(args["tk"], hash_token(token))
        self.assertEqual(args["name"], "acme")
        self.assertEqual(args["tier"], "standard")
        self.assertEqual(args["daily_quota"], 500)
        self.assertIn("expires_on", args)

    def test_create_without_days_has_no_expiry(self):
        table, code, out = MagicMock(), None, None
        code, out = self._run(["--name", "acme", "--tier", "pro"], table)
        self.assertEqual(code, 0)
        args = table.put_item.call_args.kwargs["Item"]
        self.assertEqual(args["tier"], "pro")
        self.assertNotIn("expires_on", args)

    def test_revoke_deletes_matching_name(self):
        table = MagicMock()
        table.scan.return_value = {"Items": [{"tk": "abc", "name": "acme"}]}
        code, _ = self._run(["--revoke", "acme"], table)
        self.assertEqual(code, 0)
        table.delete_item.assert_called_once_with(Key={"tk": "abc"})

    def test_revoke_unknown_name_errors(self):
        table = MagicMock()
        table.scan.return_value = {"Items": []}
        code, _ = self._run(["--revoke", "ghost"], table)
        self.assertEqual(code, 1)


if __name__ == "__main__":
    unittest.main()
