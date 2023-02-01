import os
import json
import logging
from datetime import datetime

import boto3
import natsort

# AWS configuration
REGION = "eu-west-1"  # AWS region name
BUCKET = "cc2asn-db"  # S3 bucket for curated data

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


# Store structure data to S3:
def dbstore(ccdata):
    s3 = boto3.resource(service_name="s3", region_name=REGION)
    if "DATE" in ccdata:
        # Get and remove the generation date
        yyyy = ccdata["DATE"][:4]
        mm = ccdata["DATE"][4:6]
        dd = ccdata["DATE"][6:8]
        try:
            datetime(int(yyyy), int(mm), int(dd))
            usedate = True
            logger.debug(f"RIR generation date: {yyyy}-{mm}-{dd}")
        except ValueError:
            logger.error("Invalid RIR generation date")
            usedate = False
        del ccdata["DATE"]
    else:
        logger.warning("No RIR generation date found")
        usedate = False

    fc = 0
    # For each country in the dataset
    for cc in ccdata:
        logger.debug(f"Processing country: {cc}")
        if not cc:
            continue
        files = {}
        alldata = []
        typedata = ccdata[cc]
        types = sorted(typedata.keys())

        # Create one file for each record type
        for rectype in types:
            if rectype == "IPV6":
                data = sorted(typedata[rectype])
            else:
                data = natsort.natsorted(typedata[rectype])
            alldata += data
            files[f"{cc}_{rectype}"] = data

        # Create a combined file as well
        files[f"{cc}_ALL"] = alldata

        # Write files to S3
        for filename, filedata in files.items():

            if usedate:  # Have a valid generation date for the data
                keydate = f"{yyyy}/{mm}/{dd}/{filename}"
                try:
                    s3.Object(BUCKET, keydate).put(Body="\n".join(filedata))
                    fc += 1
                except Exception as e:
                    logger.error(f"Failed to write s3:{BUCKET}/{keydate}")
                    logger.exception(e)

            keylatest = f"latest/{filename}"
            try:
                s3.Object(BUCKET, keylatest).put(Body="\n".join(filedata))
                fc += 1
            except Exception as e:
                logger.error(f"Failed to write s3:{BUCKET}/{keylatest}")
                logger.exception(e)
    return fc


# Main lambda entry point, triggered by an EventBridge rule (parser event)
def handler(event, context):
    # Try to set loglevel as defined by environment variable
    level = os.getenv("LogLevel", default="info").upper()
    if level:
        logger.setLevel(logging.getLevelName(level))
        logger.debug(f"Log level set by environment variable: {level}")

    # Process event
    logger.debug(f"Received event: {event}")
    srcbucket = event["detail"]["bucket"]["name"]
    srckey = event["detail"]["object"]["key"]
    rir = os.path.basename(srckey).split("-")[1].upper()
    logger.debug(f"Processing {srckey} from {srcbucket}")

    # Read parsed file and struture the data
    ccdata = json.loads(read_s3_file(srcbucket, srckey))
    fc = dbstore(ccdata)
    logger.info(f"Created {fc} files for {len(ccdata)} countries in {rir} region")

    return
