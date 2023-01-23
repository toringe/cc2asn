#!/usr/bin/env python
import os
import app
import logzero
import logging
from logzero import logger

if __name__ == "__main__":
    print("Run locally")
    os.environ["BUCKETDB"] = "cc2asn-db"
    logzero.loglevel(logging.getLevelName("DEBUG"))
    event = {
        "Records": [
            {
                "Sns": {
                    "Message": {
                        "sef_file": "dev/delegated-lacnic-extended-latest",
                        "bucket": "cc2asn-data",
                    },
                    "Subject": "TestInvoke",
                },
            }
        ]
    }
    # event = json.dumps(snsevent)
    # print(event["Records"][0]["Sns"]["Message"])
    context = []
    app.handler(event, context)

