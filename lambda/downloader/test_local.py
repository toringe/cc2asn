import json
import sys
import downloader
import logging

event = '{ "url": "https://ftp.ripe.net/ripe/stats/delegated-ripencc-extended-latest", "loglevel": "DEBUG" }'
context = ""

handler = logging.StreamHandler(sys.stdout)
downloader.logger.addHandler(handler)
downloader.handler(json.loads(event), context)
