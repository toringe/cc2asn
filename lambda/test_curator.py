import json
import os
import sys
import curator
import logging

event = {
    "version": "0",
    "id": "ef2a17a3-5dc2-f7a9-8785-604f92e009c9",
    "detail-type": "Object Created",
    "source": "aws.s3",
    "account": "719411478741",
    "time": "2023-01-31T11:14:42Z",
    "region": "eu-west-1",
    "resources": ["arn:aws:s3:::cc2asn-data"],
    "detail": {
        "version": "0",
        "bucket": {"name": "cc2asn-data"},
        "object": {
            "key": "parsed/delegated-afrinic-extended-latest.sef.json",
            "size": 2591859,
            "etag": "6a40279e655b97cb017947c708313423",
            "sequencer": "0063D8F820216A537E",
        },
        "request-id": "X01QR7HBD7ZD0A17",
        "requester": "719411478741",
        "source-ip-address": "84.210.146.241",
        "reason": "PutObject",
    },
}
context = ""
os.environ["LogLevel"] = "DEBUG"

handler = logging.StreamHandler(sys.stdout)
curator.logger.addHandler(handler)
curator.handler(event, context)
