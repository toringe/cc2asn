import os
import re
import json
import shutil
import hashlib
import logging
import tempfile
from urllib.request import urlopen
from urllib.parse import urlparse, unquote
from contextlib import closing


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def download(url):
    with tempfile.TemporaryDirectory() as tmpdir:
        fname = unquote(urlparse(url).path.split("/")[-1])
        logger.debug(f"Downloading {url} to {tmpdir}/{fname}")
        try:
            with closing(urlopen(url)) as response:
                with open(f"{tmpdir}/{fname}", "wb") as f:
                    shutil.copyfileobj(response, f)
        except Exception as e:
            logger.error(e)
            return None
    return f"{tmpdir}/{fname}"


# Get origin MD5 sum of the delegation file
def get_md5(url):
    try:
        with closing(urlopen(f"{url}.md5")) as r:
            md5sum = re.findall(r"[a-fA-F\d]{32}", r.read().decode("utf-8")).pop()
            logger.debug(f"Origin MD5 sum: {md5sum}")
            return md5sum
    except IndexError:
        return None


# Calculate MD5 of file
def calc_md5(filename):
    md5 = hashlib.md5()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            md5.update(chunk)
    logger.debug(f"Calculated MD5 of {filename}: {md5.hexdigest()}")
    return md5.hexdigest()


def handler(event, context):
    logger.debug("Received event %s", event)
    if "url" not in event:
        logger.error("Missing url in input event!")
    else:
        tmpfile = download(event["url"])
        if tmpfile is not None:
            logger.info("Successfully downloaded delegation file")
            logger.debug("Verifying checksum")
            if calc_md5(tmpfile) != get_md5(event["url"]):
                logger.error("Error! Invalid checksum")
            else:
                print("do more stuff here")
        else:
            logger.error("Download failed!")
    return {
        "statusCode": 200,
        "body": json.dumps({"message": "All done!"}),
    }
