# Parse a RIR delegation file according to the SEF specification
# https://ftp.ripe.net/pub/stats/ripencc/RIR-Statistics-Exchange-Format.txt

import os
import json
import math
import logging
import collections

import boto3

# AWS configuration
REGION = "eu-west-1"  # AWS region name
PREFIX = "parsed"  # Folder to store parsed files. Bucket is defined in event

#  Setup logging
logger = logging.getLogger(__name__)

# Function that reads a file from S3 and returns its content
def read_s3_file(bucket, key):
    s3 = boto3.resource(service_name="s3", region_name=REGION)
    try:
        obj = s3.Object(bucket, key)
        data = obj.get()["Body"].read().decode("utf-8").splitlines()
        logger.info(f"Successfully read {bucket}/{key}")
    except Exception as e:
        logger.error(f"Failed to read {bucket}/{key}")
        logger.exception(e)
        data = None
    return data


# Function that writes a file to S3
def write_s3_file(bucket, key, data):
    s3 = boto3.resource(service_name="s3", region_name=REGION)
    try:
        obj = s3.Object(bucket, key)
        obj.put(Body=data)
        logger.info(f"Successfully wrote {bucket}/{key}")
    except Exception as e:
        logger.error(f"Failed to write {bucket}/{key}")
        logger.exception(e)
    return


# SEF (Statistics Exchange Format) parser
def parser(sefdata):
    logger.debug("Parsing SEF data")

    # Store parsed data in an ordered dictionary
    ccdata = collections.OrderedDict()

    # Data generation date (set by RIR)
    dgendate = None

    for ln in sefdata:

        # Remove all whitespace
        ln = ln.strip()

        # Skip all comments
        if ln.startswith("#"):
            continue

        # Skip summary
        if ln.endswith("summary"):
            continue

        # Skip non-allocated records
        if ln.rstrip("|").endswith("available") or ln.rstrip("|").endswith("reserved"):
            continue

        # Version line
        # 0      |1       |2     |3      |4        |5      |6
        # version|registry|serial|records|startdate|enddate|UTCoffset
        if ln[0].isdigit():
            dgendate = ln.split("|")[5]
            continue

        # Extract records
        # 0       |1 |2   |3    |4    |5   |6     |7
        # registry|cc|type|start|value|date|status[|extensions...]
        elements = list(map(lambda x: x.upper(), ln.split("|")))
        cc = elements[1]
        iptype = elements[2]
        start = str(elements[3])
        value = int(elements[4])

        # Process IP and ASNs records
        record = ""
        if iptype == "IPV4" or iptype == "IPV6":
            if ":" not in start:
                value = int(math.ceil(32 - math.log(value, 2)))
            record = start + "/" + str(value)
        elif iptype == "ASN":
            record = "AS" + start
        else:
            logger.warning(f"Undefined record type: {iptype}")

        # Structurize records
        typedata = {}
        if cc in ccdata:
            typedata = ccdata[cc]
            if iptype in typedata:
                data = typedata[iptype]
                data.append(record)
                typedata[iptype] = data
                ccdata[cc] = typedata
            else:
                typedata[iptype] = [record]
                ccdata[cc] = typedata
        else:
            typedata[iptype] = [record]
            ccdata[cc] = typedata
    logger.info(f"Parsed {len(sefdata)} SEF entries into {len(ccdata)} countries")

    # Add metadata
    ccdata.update({"DATE": dgendate})  # RIR Generation date

    return ccdata


# Main lambda entry point, trigged by an EventBridge rule (new S3 obj created)
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
    logger.debug(f"Processing {srckey} from {srcbucket}")

    # Parse and store SEF data
    ccdata = parser(read_s3_file(srcbucket, srckey))
    parsedfile = f"{PREFIX}/{os.path.basename(srckey)}.json"
    write_s3_file(srcbucket, parsedfile, json.dumps(ccdata))

    return
