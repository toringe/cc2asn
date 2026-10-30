"""Token lookup and atomic daily-quota consumption against DynamoDB."""

import hashlib
import datetime

from botocore.exceptions import ClientError


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


class TokenStore:
    def __init__(self, table):
        self.table = table

    def authorize(self, token: str, today: str) -> dict | None:
        response = self.table.get_item(Key={"tk": hash_token(token)})
        item = response.get("Item")
        if item is None:
            return None
        expires_on = item.get("expires_on")
        if expires_on is not None and expires_on < today:
            return None
        return item

    def consume(self, token: str, today: str) -> bool:
        tk = hash_token(token)
        # New-day path: (no used_date yet) or (used_date belongs to an earlier day)
        try:
            self.table.update_item(
                Key={"tk": tk},
                UpdateExpression="SET used_date = :today, used_today = :one",
                ConditionExpression="attribute_not_exists(used_date) OR used_date <> :today",
                ExpressionAttributeValues={":today": today, ":one": 1},
            )
            return True
        except self.table.meta.client.exceptions.ConditionalCheckFailedException:
            pass
        # Same-day path: require budget remains, then increment atomically.
        try:
            self.table.update_item(
                Key={"tk": tk},
                UpdateExpression="ADD used_today :incr",
                ConditionExpression="used_today < :quota",
                ExpressionAttributeValues={":incr": 1, ":quota": self._quota(tk)},
            )
            return True
        except (self.table.meta.client.exceptions.ConditionalCheckFailedException, ClientError):
            return False

    def _quota(self, tk: str) -> int:
        item = self.table.get_item(Key={"tk": tk}).get("Item")
        return int(item.get("daily_quota", 0))
