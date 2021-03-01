# Core modules
import os
import re
import json
import shutil
import logging
import hashlib
from contextlib import closing
from urllib.request import urlopen

# Other modules
import boto3
import logzero
from logzero import logger
from chalice import Chalice
from chalice.app import Cron

# Initialize the chalice framework
stage = os.getenv("STAGE").lower()
app = Chalice(app_name=f"cc2asn-rir-dl-{stage}")
if stage == "dev":
    app.debug = True

# Set loglevel as defined by environment variable (default is Error)
level = os.getenv("LOGLEVEL").upper()
if level:
    logzero.loglevel(logging.getLevelName(level))

# AWS resources
s3 = boto3.resource(service_name="s3", region_name=os.getenv("REGION"))
sns = boto3.client("sns", region_name=os.getenv("REGION"))

# RIR Delegation files
RIRDF = {
    "ARIN": os.getenv("ARIN"),
    "RIPE": os.getenv("RIPE"),
    "AFRINIC": os.getenv("AFRINIC"),
    "APNIC": os.getenv("APNIC"),
    "LACNIC": os.getenv("LACNIC"),
}

# Publish SNS message (message should be a dict)
def publish_sns(message, arn):
    logger.debug(f"SNS Message: {message}")
    response = sns.publish(
        TargetArn=arn,
        Message=json.dumps({"default": json.dumps(message)}),
        MessageStructure="json",
    )
    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        return True
    else:
        return False


# Save local temp file to S3
def save_to_s3(tmpfile, bucket, key):
    try:
        s3.Object(bucket, key).put(Body=open(tmpfile, "rb"))
        logger.debug(f"Stored on S3: {bucket}/{key}")
        return True
    except Exception as e:
        logger.error("Failed to store to S3")
        logger.exception(e)
        return False


# Calculate MD5 of file
def calc_md5(filename):
    md5 = hashlib.md5()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            md5.update(chunk)
    return md5.hexdigest()


# Get the original MD5 sum of the delegation file
def get_md5(url):
    try:
        with closing(urlopen(f"{url}.md5")) as r:
            return re.findall(r"[a-fA-F\d]{32}", r.read().decode("utf-8")).pop()
    except IndexError:
        return None


# Download a file from the specified URL
def download(url, filepath):
    try:
        if not os.path.exists(os.path.dirname(filepath)):
            os.makedirs(os.path.dirname(filepath))

        with urlopen(url) as response, open(filepath, "wb") as tmpfile:
            logger.debug(f"URL opened: {url}")
            shutil.copyfileobj(response, tmpfile)
            logger.debug(f"Saved temporarily to {filepath}")
        return True
    except Exception as e:
        logger.exception(e)
        logger.error(f"Failed to download: {url}")
        return False


# Remove all files from the temporary local storage
def cleanup(tmpdir):
    try:
        shutil.rmtree(tmpdir)
        logger.debug(f"Cleaned up {tmpdir}")
    except Exception as e:
        logger.error(f"Failed to clean up {tmpdir}")
        logger.exception(e)


# Scheduled Lambda function to spawn off a separate Lambda for each RIR
@app.schedule(Cron(0, 3, "*", "*", "?", "*"))
def dispatcher(event, context):
    snsarn = os.getenv("RIRSNS")
    for rir, url in RIRDF.items():
        message = {"rir_name": rir, "rir_url": url}
        if not publish_sns(message, snsarn):
            logger.error(f"Failed to publish to SNS (snsarn)")
            return {"Status": "Failed"}
    return {"Status": "OK"}


# Lambda function to download and store latest delegation file from a RIR
@app.on_sns_message(topic=f"cc2asn-rir-dl-{stage}")
def worker(event):
    logger.debug(f"SNS message: {event.message}")
    msg = json.loads(event.message)
    tmpfile = f"{os.getenv('TMPDIR')}/{msg['rir_name']}"
    logger.debug(f"Downloading delegation file from {msg['rir_name']}")
    if download(msg["rir_url"], tmpfile):
        logger.debug("Verifying checksum")
        if calc_md5(tmpfile) != get_md5(msg["rir_url"]):
            logger.error("Error! Invalid checksum")
        else:
            dfname = msg["rir_url"].split("/")[-1]
            save_to_s3(tmpfile, os.getenv("BUCKET"), f"{stage}/{dfname}")
        cleanup(os.getenv("TMPDIR"))
    return
