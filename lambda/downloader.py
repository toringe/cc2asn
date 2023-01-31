# Download and verify a RIR delegation file as specified in the event URL

import os
import re
import json
import time
import shutil
import hashlib
import logging
from contextlib import closing
from urllib.request import urlopen
from urllib.parse import urlparse, unquote

import boto3

# AWS configuration
REGION = "eu-west-1"  # AWS region name
BUCKET = "cc2asn-data"  # S3 bucket name
PREFIX = "RIR-SEF"  # Folder in bucket

#  Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Download file from URL
def download(url, tmpdir):
    fname = unquote(urlparse(url).path.split("/")[-1])
    logger.debug(f"Downloading {url} to {tmpdir}/{fname}")
    try:
        with urlopen(url) as response:
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
            logger.debug(f"Origin MD5: {md5sum}")
            return md5sum
    except IndexError:
        return None


# Calculate MD5 of file
def calc_md5(filename):
    md5 = hashlib.md5()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            md5.update(chunk)
    logger.debug(f"Calculated MD5: {md5.hexdigest()}")
    return md5.hexdigest()


# Save local temp file to S3
def save_to_s3(tmpfile, bucket, key):
    try:
        logger.debug(f"Uploading {tmpfile} to S3://{bucket}/{key}")
        s3 = boto3.resource(service_name="s3", region_name=REGION)
        s3.Object(bucket, key).put(Body=open(tmpfile, "rb"))
        return f"S3://{bucket}/{key}"
    except Exception as e:
        logger.exception(e)
        return None


# Create temporary space
def mktmpdir():
    tmpdir = "/tmp/cc2asn." + str(int(time.time()))
    try:
        os.mkdir(tmpdir)
        logger.debug(f"Created {tmpdir}")
    except FileExistsError:
        logger.warning(f"{tmpdir} already exists")
        pass
    return tmpdir


# Remove all files from the temporary local storage
def cleanup(tmpdir):
    try:
        shutil.rmtree(tmpdir)
        logger.debug(f"Cleaned up {tmpdir}")
    except Exception as e:
        logger.error(f"Failed to clean up {tmpdir}")
        logger.exception(e)


# Main Lambda entry point
def handler(event, context):
    # Set log level
    try:
        logger.setLevel(getattr(logging, event["loglevel"]))
    except (KeyError, AttributeError) as e:
        logger.setLevel(logging.INFO)
        logger.warning(
            f"No or invalid log level specified in event. Defaulting to INFO"
        )

    # Process event
    logger.debug("Received event %s", event)
    if "url" not in event:
        logger.error("Missing url in input event!")
    else:
        tmpdir = mktmpdir()
        tmpfile = download(event["url"], tmpdir)
        if tmpfile is not None:
            logger.info("Successfully downloaded delegation file")
            if calc_md5(tmpfile) != get_md5(event["url"]):
                logger.error("Error! Invalid checksum")
            else:
                logger.info("Checksum OK")
                s3path = save_to_s3(
                    tmpfile, BUCKET, f"{PREFIX}/{os.path.basename(tmpfile)}"
                )
                if s3path is not None:
                    logger.info(f"Successfully stored on {s3path}")
                else:
                    logger.error("Error! Could not store file on S3")
            cleanup(tmpdir)
        else:
            logger.error("Failed to download delegation file")
    return
