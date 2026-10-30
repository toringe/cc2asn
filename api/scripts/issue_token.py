"""Create or revoke CC2ASN API historic-lookup tokens (see README).

Usage:
  python scripts/issue_token.py --name NAME [--tier historic|historic-pro]
      [--quota N] [--days N]
  python scripts/issue_token.py --revoke NAME
"""

import argparse
import datetime
import os
import secrets
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import boto3  # noqa: E402
from boto3.dynamodb.conditions import Attr  # noqa: E402

from lib.tokens import hash_token  # noqa: E402

TABLE = "cc2asn-tokens"


def _parse_args(argv):
    parser = argparse.ArgumentParser(description="Issue or revoke CC2ASN API tokens")
    parser.add_argument("--name", help="token description, e.g. customer/company name")
    parser.add_argument("--tier", choices=("historic", "historic-pro"), default="historic")
    parser.add_argument("--quota", type=int, default=100, help="allowed historic lookups per day")
    parser.add_argument(
        "--days", type=int, default=None, help="token validity in days (no expiry if omitted)"
    )
    parser.add_argument("--revoke", metavar="NAME", help="revoke all tokens with this name")
    return parser.parse_args(argv)


def _table():
    name = os.environ.get("CC2ASN_TOKENS_TABLE", TABLE)
    return boto3.resource("dynamodb").Table(name)


def _create(args, table) -> int:
    token = secrets.token_hex(16)
    item = {
        "tk": hash_token(token),
        "name": args.name,
        "tier": args.tier,
        "daily_quota": args.quota,
    }
    if args.days is not None:
        item["expires_on"] = (
            datetime.date.today() + datetime.timedelta(days=args.days)
        ).isoformat()
    table.put_item(Item=item)
    # The plaintext token is shown exactly once; only its hash is stored.
    print(token)
    return 0


def _revoke(args, table) -> int:
    response = table.scan(FilterExpression=Attr("name").eq(args.revoke))
    items = response.get("Items", [])
    if not items:
        print(f"No tokens found with name: {args.revoke}", file=sys.stderr)
        return 1
    for item in items:
        table.delete_item(Key={"tk": item["tk"]})
    print(f"Revoked {len(items)} token(s) for {args.revoke}")
    return 0


def main(argv=None) -> int:
    args = _parse_args(argv)
    if args.revoke:
        return _revoke(args, _table())
    if not args.name:
        print("--name is required to create a token (use --revoke to remove)", file=sys.stderr)
        return 1
    return _create(args, _table())


if __name__ == "__main__":
    sys.exit(main())
