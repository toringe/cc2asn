"""
Parse RIR SEF files and output country spesific data files.
Spec: ftp.ripe.net/pub/stats/ripencc/RIR-Statistics-Exchange-Format.txt

Author: Tor Inge Skaar

"""
# Core modules
import sys
import math
import collections

# Other modules
import boto3
import logzero
import natsort
from logzero import logger
from chalice import Chalice
from chalice.app import Cron

# Initialize the chalice framework
stage = os.getenv("STAGE").lower()
app = Chalice(app_name=f"cc2asn-sef-parser-{stage}")
if stage == "dev":
    app.debug = True

# Set loglevel as defined by environment variable (default is Error)
level = os.getenv("LOGLEVEL").upper()
if level:
    logzero.loglevel(logging.getLevelName(level))


# Statistics Exchange Format (SEF) parser
def sef_parse(sefdata):
    for ln in sefdata:

        # Remove all whitespace
        ln = ln.strip()

        # Skip all comments
        if ln.startswith("#"):
            continue

        # Skip header
        if ln[0].isdigit():
            continue

        # Skip summary
        if ln.endswith("summary"):
            continue

        # Skip not allocated records
        if ln.rstrip("|").endswith("available") or ln.rstrip("|").endswith("reserved"):
            continue

        # Extract records
        # 0       |1 |2   |3    |4
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


@app.lambda_function()
def handler():
    s3 = boto3.resource(service_name="s3", region_name=os.getenv("REGION"))
    bucket_df = s3.Bucket(os.getenv("BUCKETDF"))  # Delegation files
    bucket_db = s3.Bucket(os.getenv("BUCKETDB"))  # Parsed and structured data
    stats = {}
    for content in bucket_df.objects.all():
        if content.key.endswith("latest"):
            try:  # Get the delegation file from S3...
                obj = s3.Object(bucket_df.name, content.key)
                data = obj.get()["Body"].read().decode("utf-8").splitlines()
                stats[content.key.split("-")[1]] = len(data)
            except Exception as e:
                logger.error(f"Failed to read {bucket_df.name}/{content.key}")
                logger.exception(e)
                return {"status": "Failed"}
            # ...and parse it
            sef_parse(data)

    return {"status": "OK"}
