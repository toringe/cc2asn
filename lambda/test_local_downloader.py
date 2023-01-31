#!/usr/bin/env python3
import json
import sys
import downloader
import logging

# Download events
# event = '{ "url": "https://ftp.ripe.net/ripe/stats/delegated-ripencc-extended-latest", "loglevel": "DEBUG" }'
event = '{ "url": "https://ftp.lacnic.net/pub/stats/lacnic/delegated-lacnic-extended-latest", "loglevel": "DEBUG" }'
# event = '{ "url": "https://ftp.afrinic.net/pub/stats/afrinic/delegated-afrinic-extended-latest", "loglevel": "DEBUG" }'
# event = '{ "url": "https://ftp.arin.net/pub/stats/arin/delegated-arin-extended-latest", "loglevel": "DEBUG" }'
# event = '{ "url": "https://ftp.apnic.net/pub/stats/apnic/delegated-apnic-extended-latest", "loglevel": "DEBUG" }'
context = ""

handler = logging.StreamHandler(sys.stdout)
downloader.logger.addHandler(handler)
downloader.handler(json.loads(event), context)
