"""Query parsing and response rendering with whois-compatible semantics."""

import datetime
import json
import re
from dataclasses import dataclass

_CC_RE = re.compile(r"^[A-Z]{2}$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

RECORD_TYPES = ("ASN", "IPV4", "IPV6", "ALL")


@dataclass(frozen=True)
class Query:
    cc: str
    rectype: str
    date: str | None = None


def _valid_date(date: str) -> bool:
    if not _DATE_RE.match(date):
        return False
    try:
        datetime.date.fromisoformat(date)
    except ValueError:
        return False
    return True


def parse_query(path: str) -> Query | None:
    path = path.split("?", 1)[0]
    segments = path.split("/")  # leading "/" yields an empty first segment
    if len(segments) < 2 or segments[0] != "":
        return None
    segments = [s for s in segments[1:]]
    if not 1 <= len(segments) <= 3 or any(s == "" for s in segments):
        return None

    first = segments[0].upper()
    if _valid_date(first):
        date = first
        options = segments[1:]
        if not 1 <= len(options) <= 2:
            return None  # a date alone does not name a country; >2 is not a form
        cc_candidate = options[-1]
        rectype = options[0].upper() if len(options) == 2 else "ASN"
    else:
        date = None
        options = segments
        if len(options) > 2:
            return None
        cc_candidate = options[-1]
        rectype = options[0].upper() if len(options) == 2 else "ASN"

    cc = cc_candidate.upper()
    if not _CC_RE.match(cc):
        return None
    if rectype not in RECORD_TYPES:
        return None
    return Query(cc=cc, rectype=rectype, date=date)


def render_plain(body: str) -> str:
    stripped = body.rstrip("\n")
    return stripped + "\n" if stripped else ""


def render_json(query: Query, data_date: str, body: str) -> str:
    records = [line for line in body.splitlines() if line]
    return json.dumps(
        {"cc": query.cc, "type": query.rectype, "date": data_date, "records": records},
        indent=2,
    ) + "\n"
