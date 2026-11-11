import os
import json
import hashlib
import logging
from datetime import datetime

import boto3
import natsort

# AWS configuration
REGION = "eu-west-1"  # AWS region name
BUCKET = "cc2asn-db"  # S3 bucket for curated data

# Parsed files (written by the parser) for every RIR. Each curator run merges
# all of them: a country can appear in several registries' files, so writing
# one registry's records alone would overwrite the others' (see fix-curator).
RIRS = ["afrinic", "apnic", "arin", "lacnic", "ripencc"]
PARSED_KEY = "parsed/delegated-{rir}-extended-latest.json"

#  Setup logging
logger = logging.getLogger(__name__)


# Function that reads a file from S3 and returns its content
def read_s3_file(bucket, key):
    s3 = boto3.resource(service_name="s3", region_name=REGION)
    try:
        obj = s3.Object(bucket, key)
        data = obj.get()["Body"].read().decode("utf-8")
        logger.info(f"Successfully read {bucket}/{key}")
    except Exception as e:
        logger.error(f"Failed to read {bucket}/{key}")
        logger.exception(e)
        data = None
    return data


# Merge the parsed data of several RIRs into one {cc: {type: [records]}}
# structure. Records are de-duplicated and sorted (IPv6 lexically, the rest
# naturally). Returns the merged data and the newest RIR generation date.
def merge_parsed(parsed_list):
    merged = {}
    dates = []
    for parsed in parsed_list:
        for cc, typedata in parsed.items():
            if cc == "DATE":
                if typedata:
                    dates.append(typedata)
                continue
            if not cc:
                continue
            cctypes = merged.setdefault(cc, {})
            for rectype, records in typedata.items():
                cctypes.setdefault(rectype, set()).update(records)

    ccdata = {}
    for cc, typedata in merged.items():
        ccdata[cc] = {}
        for rectype, records in typedata.items():
            if rectype == "IPV6":
                ccdata[cc][rectype] = sorted(records)
            else:
                ccdata[cc][rectype] = natsort.natsorted(records)
    return ccdata, max(dates, default=None)


# Build the per-country file contents: <CC>_<TYPE> and the combined <CC>_ALL
def build_files(ccdata):
    files = {}
    for cc, typedata in ccdata.items():
        alldata = []
        for rectype in sorted(typedata):
            data = typedata[rectype]
            alldata += data
            files[f"{cc}_{rectype}"] = "\n".join(data)
        files[f"{cc}_ALL"] = "\n".join(alldata)
    return files


def md5_etag(body):
    return '"' + hashlib.md5(body.encode("utf-8")).hexdigest() + '"'


# Decide what to write and delete so a prefix matches `files` exactly.
# `existing` maps file name -> S3 ETag. Single-part PUTs to an SSE-S3 bucket
# have ETag == MD5 of the body, so unchanged files are skipped.
def plan_sync(files, existing):
    puts = [name for name, body in files.items() if existing.get(name) != md5_etag(body)]
    deletes = sorted(name for name in existing if name not in files)
    return puts, deletes


# List the objects under a prefix as {file name: ETag}
def list_etags(client, prefix):
    existing = {}
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            name = obj["Key"][len(prefix):]
            if name and "/" not in name:
                existing[name] = obj["ETag"]
    return existing


# Make s3://BUCKET/<prefix> hold exactly `files`. Returns (written, deleted).
def sync_prefix(client, prefix, files):
    puts, deletes = plan_sync(files, list_etags(client, prefix))
    written = 0
    for name in puts:
        key = prefix + name
        try:
            client.put_object(Bucket=BUCKET, Key=key, Body=files[name].encode("utf-8"))
            written += 1
        except Exception as e:
            logger.error(f"Failed to write s3:{BUCKET}/{key}")
            logger.exception(e)
    deleted = 0
    for i in range(0, len(deletes), 1000):
        batch = [{"Key": prefix + name} for name in deletes[i:i + 1000]]
        resp = client.delete_objects(Bucket=BUCKET, Delete={"Objects": batch, "Quiet": True})
        for err in resp.get("Errors", []):
            logger.error(f"Failed to delete s3:{BUCKET}/{err.get('Key')}: {err.get('Message')}")
        deleted += len(batch) - len(resp.get("Errors", []))
    logger.info(f"{prefix}: {written} written, {len(files) - len(puts)} unchanged, {deleted} deleted")
    return written, deleted


# Convert a RIR generation date (yyyymmdd) to a key prefix, or None if invalid
def date_prefix(date):
    if not date:
        logger.warning("No RIR generation date found")
        return None
    try:
        datetime(int(date[:4]), int(date[4:6]), int(date[6:8]))
    except ValueError:
        logger.error(f"Invalid RIR generation date: {date}")
        return None
    return f"{date[:4]}/{date[4:6]}/{date[6:8]}/"


# Main lambda entry point, triggered by an EventBridge rule (parser event)
def handler(event, context):
    # Try to set loglevel as defined by environment variable
    level = os.getenv("LogLevel", default="info").upper()
    if level:
        logger.setLevel(logging.getLevelName(level))
        logger.debug(f"Log level set by environment variable: {level}")

    # Process event. The triggering file only tells us that something changed;
    # the output is always rebuilt from all RIRs' parsed files.
    logger.debug(f"Received event: {event}")
    srcbucket = event["detail"]["bucket"]["name"]
    srckey = event["detail"]["object"]["key"]
    logger.info(f"Triggered by {srcbucket}/{srckey}")

    # Read every RIR's parsed file. Refuse to publish a partial merge: it
    # would drop (and delete) a whole registry's records.
    parsed_list = []
    for rir in RIRS:
        key = PARSED_KEY.format(rir=rir)
        raw = read_s3_file(srcbucket, key)
        if raw is None:
            raise RuntimeError(f"Missing parsed file {srcbucket}/{key}; not publishing")
        parsed = json.loads(raw)
        if not any(cc != "DATE" for cc in parsed):
            raise RuntimeError(f"Parsed file {srcbucket}/{key} has no countries; not publishing")
        parsed_list.append(parsed)

    ccdata, date = merge_parsed(parsed_list)
    files = build_files(ccdata)

    client = boto3.client("s3", region_name=REGION)
    prefixes = ["latest/"]
    dated = date_prefix(date)
    if dated:
        prefixes.insert(0, dated)
    for prefix in prefixes:
        sync_prefix(client, prefix, files)

    logger.info(f"Curated {len(files)} files for {len(ccdata)} countries from {len(RIRS)} RIRs (date {date})")
    return
