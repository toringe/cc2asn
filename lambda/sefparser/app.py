"""
Parse RIR SEF files and output country spesific data files.
Spec: ftp.ripe.net/pub/stats/ripencc/RIR-Statistics-Exchange-Format.txt

Author: Tor Inge Skaar

"""
# Core modules
import os
import sys
import math
import json
import logging
import collections
from datetime import datetime

# Other modules
import boto3
import logzero
import natsort
from logzero import logger
from chalice import Chalice

# Initialize the chalice framework
stage = os.getenv("STAGE", default="dev").lower()
app = Chalice(app_name=f"cc2asn-sef-parser-{stage}")
if stage == "dev":
    app.debug = True

# Set loglevel as defined by environment variable (default is Error)
level = os.getenv("LOGLEVEL", default="error").upper()
if level:
    print(f"log level: {level}")
    logzero.loglevel(logging.getLevelName(level))

# Statistics Exchange Format (SEF) parser
def parser(sefdata):

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

        # Process prefixes and ASNs
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


# Store and structure parsed data onto S3:
def dbstore(ccdata, db_bucket, s3):

    if "DATE" in ccdata:
        # Get and remove the generation date
        yyyy = ccdata["DATE"][:4]
        mm = ccdata["DATE"][4:6]
        dd = ccdata["DATE"][6:8]
        try:
            datetime(int(yyyy), int(mm), int(dd))
            usedate = True
        except ValueError:
            logger.error("Invalid RIR generation date")
            usedate = False
        del ccdata["DATE"]
    else:
        usedate = False

    # For each country in the dataset
    for cc in ccdata:
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
                    logger.debug(f"Writing data to s3:{db_bucket}/{keydate}")
                    s3.Object(db_bucket, keydate).put(Body="\n".join(filedata))
                except Exception as e:
                    logger.error(f"Failed to write s3:{db_bucket}/{keydate}")
                    logger.exception(e)
                    return {"status": "Failed"}

            keylatest = f"latest/{filename}"
            try:
                logger.debug(f"Writing data to s3:{db_bucket}/{keylatest}")
                s3.Object(db_bucket, keylatest).put(Body="\n".join(filedata))
            except Exception as e:
                logger.error(f"Failed to write s3:{db_bucket}/{keylatest}")
                logger.exception(e)
                return {"status": "Failed"}


# Lambda handler function (triggered by SNS event)
@app.on_sns_message(topic=f"cc2asn-sef-file-{stage}")
def handler(event):
    logger.info(f"SNS message: {event.message}")
    sef_file = event.message["sef_file"]  # SEF file defined by worker
    bucket_df = event.message["bucket"]  # SEF data bucket defined by worker
    bucket_db = os.getenv("BUCKETDB", default="cc2asn-db")  # Database bucket

    # Read SEF file on S3
    s3 = boto3.resource(
        service_name="s3", region_name=os.getenv("REGION", default="us-east-1")
    )
    try:
        obj = s3.Object(bucket_df, sef_file)
        data = obj.get()["Body"].read().decode("utf-8").splitlines()
        logger.info(f"Successfully read {bucket_df}/{sef_file}")
    except Exception as e:
        logger.error(f"Failed to read {bucket_df}/{sef_file}")
        logger.exception(e)
        return {"status": "Failed"}

    # Parse SEF file
    ccdata = parser(data)

    # Structure and store data
    dbstore(ccdata, bucket_db, s3)

    return {"status": "OK"}
