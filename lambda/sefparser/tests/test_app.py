import os
import json
from chalice.test import Client
from app import app


def test_sns_handler():
    with Client(app, stage_name="dev") as client:
        msg = {
            "sef_file": "dev/delegated-afrinic-extended-latest",
            "bucket": "cc2asn-data",
        }
        response = client.lambda_.invoke(
            "handler", client.events.generate_sns_event(message=json.dumps(msg))
        )

        assert response.payload == {"status": "OK"}
