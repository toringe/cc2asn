import json
import os
import sys
import curator
import logging

event = {
    "version": "0",
    "id": "4c96836d-003d-998d-e0e6-becb8c02b02c",
    "detail-type": "CC2ASN-SEF-Data",
    "source": "CC2ASN-Parser",
    "account": "719411478741",
    "time": "2023-01-30T19:53:20Z",
    "region": "eu-west-1",
    "resources": [],
    "detail": {
        "bucket": "cc2asn-data",
        "key": "parsed/delegated-ripencc-extended-latest.json",
    },
}
context = ""
os.environ["LogLevel"] = "DEBUG"

handler = logging.StreamHandler(sys.stdout)
curator.logger.addHandler(handler)
curator.handler(event, context)
