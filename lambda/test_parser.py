import json
import os
import sys
import parser
import logging

delegation_file = "delegated-ripencc-extended-latest"
event = {
    "version": "0",
    "id": "193f49ac-d582-51d1-3aca-67c0bf54a745",
    "detail-type": "Object Created",
    "source": "aws.s3",
    "account": "719411478741",
    "time": "2023-01-30T14:04:33Z",
    "region": "eu-west-1",
    "resources": ["arn:aws:s3:::cc2asn-data"],
    "detail": {
        "version": "0",
        "bucket": {"name": "cc2asn-data"},
        "object": {
            "key": f"{delegation_file}",
            "size": 1817,
            "etag": "8dece44db2d8aa29fa9367a17e327672",
            "version-id": "5G2q4qWw2ImSeAm8CucAil.7_iL6uFKc",
            "sequencer": "0063D7CE71A7D76012",
        },
        "request-id": "ZR9RGS5N9YTHC6FN",
        "requester": "719411478741",
        "source-ip-address": "84.210.146.241",
        "reason": "PutObject",
    },
}
context = ""
os.environ["LogLevel"] = "DEBUG"

handler = logging.StreamHandler(sys.stdout)
parser.logger.addHandler(handler)
parser.handler(event, context)
