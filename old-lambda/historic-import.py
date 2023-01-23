#!/usr/bin/env python
################################################################################
# HISTORIC IMPORT OF RIR DELEGATION FILES (SEF)
################################################################################
import os
import bz2
import sys
import gzip
import math
import boto3
import shutil
import tempfile
import logzero
import logging
import natsort
import requests
import collections
from alive_progress import alive_bar
from logzero import logger
from datetime import datetime
from urllib.request import urlopen

# Configuration
# AWS credentials should be configured in ~/.aws/credentials
REGION = "eu-west-1"
BUCKET = "cc2asn-db"
LOGLEVEL = "DEBUG"

# RIR Sources
sources = {
    "arin": "https://ftp.arin.net/pub/stats/arin/",
    "ripe": "https://ftp.ripe.net/pub/stats/ripencc/",
    "apnic": "https://ftp.apnic.net/stats/apnic/",
    "lacnic": "https://ftp.lacnic.net/pub/stats/lacnic/",
    "afrinic": "https://ftp.afrinic.net/pub/stats/afrinic/",
}

# Get full URL for ARIN delegation file given specific date
def arin(src, date):
    now = datetime.now()
    if date < datetime(2003, 11, 20):
        logger.warning("No supported ARIN delegation file before 2003-11-20")
        url = None
    elif date.year == now.year or date.year == now.replace(year=now.year - 1).year:
        url = f"{src}delegated-arin-extended-{date.strftime('%Y%m%d')}"
    elif date < datetime(2013, 3, 5):
        logger.warning("No extended delegation file before 2013-03-05 on ARIN")
        url = f"{src}archive/{date.year}/delegated-arin-{date.strftime('%Y%m%d')}.gz"
    else:
        url = f"{src}archive/{date.year}/delegated-arin-extended-{date.strftime('%Y%m%d')}.gz"
    return url


# Get full URL for RIPE delegation file given specific date
def ripe(src, date):
    if date < datetime(2004, 1, 2):
        logger.warning("No supported RIPE delegation file before 2004-01-02")
        url = None
    elif date < datetime(2010, 4, 22):
        logger.warning("No extended delegation file before 2010-04-22 on RIPE")
        url = f"{src}{date.year}/delegated-ripencc-{date.strftime('%Y%m%d')}.bz2"
    else:
        url = (
            f"{src}{date.year}/delegated-ripencc-extended-{date.strftime('%Y%m%d')}.bz2"
        )
    return url


# Get full URL for APNIC delegation file given specific date
def apnic(src, date):
    if date < datetime(2003, 5, 1):
        logger.warning("No supported APNIC delegation file before 2003-05-01")
        url = None
    elif date < datetime(2008, 2, 14):
        logger.warning("No extended delegation file before 2008-02-14 on APNIC")
        url = f"{src}{date.year}/delegated-apnic-{date.strftime('%Y%m%d')}.gz"
    else:
        url = f"{src}{date.year}/delegated-apnic-extended-{date.strftime('%Y%m%d')}.gz"
    return url


# Get full URL for LACNIC delegation file given specific date
def lacnic(src, date):
    if date < datetime(2004, 1, 1):
        logger.warning("No supported LACNIC delegation file before 2004-01-01")
        url = None
    elif date < datetime(2012, 6, 28):
        logger.warning("No extended delegation file before 2012-06-28 on LACNIC")
        url = f"{src}/delegated-lacnic-{date.strftime('%Y%m%d')}"
    else:
        url = f"{src}/delegated-lacnic-extended-{date.strftime('%Y%m%d')}"
    return url


# Get full URL for AFRINIC delegation file given specific date
def afrinic(src, date):
    if date < datetime(2005, 2, 18):
        logger.warning("No supported AFRINIC delegation file before 2005-02-18")
        url = None
    elif date < datetime(2012, 10, 2):
        logger.warning("No extended delegation file before 2012-10-02 on AFRINIC")
        url = f"{src}{date.year}/delegated-afrinic-{date.strftime('%Y%m%d')}"
    else:
        url = f"{src}{date.year}/delegated-afrinic-extended-{date.strftime('%Y%m%d')}"
    return url


# Download a file from the specified URL and return the contents
# def download(url):
#    # if os.path.isfile("test.gz"):
#    #    with gzip.open("test.gz", "rt") as f:
#    #        logger.debug("Return cached test.gz")
#    #        return f.read().splitlines()
#    # if os.path.isfile("test.bz2"):
#    #    with bz2.open("test.bz2", "rt") as f:
#    #        logger.debug("Return cached test.bz2")
#    #        return f.read().splitlines()
#
#    with alive_bar(
#        3,
#        title=" - Get raw data   ",
#        bar="filling",
#        spinner="dots",
#        monitor=False,
#        stats_end=False,
#    ) as bar:
#        logger.debug(f"Download: {url}")
#        try:
#            with tempfile.NamedTemporaryFile() as tmp:
#                logger.debug(f"Temp file: {tmp.name}")
#                bar()
#                with urlopen(url) as response, open(tmp.name, "wb") as out_file:
#                    logger.debug(f"Got HTTP {response.code} back")
#                    shutil.copyfileobj(response, out_file)
#                    bar()
#                    if url.endswith(".gz"):
#                        logger.debug("Uncompress gzip")
#                        with gzip.open(out_file.name, "rt") as f:
#                            bar()
#                            return f.read().splitlines()
#                    elif url.endswith(".bz2"):
#                        logger.debug("Uncompress bzip2")
#                        with bz2.open(out_file.name, "rt") as f:
#
#                            bar()
#                            return f.read().splitlines()
#                    else:
#                        logger.debug("No compression")
#                        with open(out_file.name, "rt") as f:
#                            bar()
#                            return f.read().splitlines()
#        except Exception as e:
#            logger.exception(e)
#            logger.error(f"Failed to download: {url}")
#            return False


# Statistics Exchange Format (SEF) parser
def parser(sefdata):

    # Store parsed data in an ordered dictionary
    ccdata = collections.OrderedDict()

    # Data generation date (set by RIR)
    dgendate = None
    with alive_bar(
        len(sefdata),
        title=" - Parsing records",
        bar="filling",
        spinner="dots",
        stats_end=False,
    ) as bar:
        for ln in sefdata:

            # Remove all whitespace
            ln = ln.strip()

            # Skip all comments
            if ln.startswith("#"):
                bar()
                continue

            # Skip summary
            if ln.endswith("summary"):
                bar()
                continue

            # Skip non-allocated records
            if ln.rstrip("|").endswith("available") or ln.rstrip("|").endswith(
                "reserved"
            ):
                bar()
                continue

            # Version line
            # 0      |1       |2     |3      |4        |5      |6
            # version|registry|serial|records|startdate|enddate|UTCoffset
            if ln[0].isdigit():
                dgendate = ln.split("|")[5]
                bar()
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
            bar()

    # Add metadata
    ccdata.update({"DATE": dgendate})  # RIR Generation date

    return ccdata


# Get total number of files to be stored
def fcount(ccdata):
    fc = 0
    for cc in ccdata:
        fc += 1
        try:
            for _ in ccdata[cc].keys():
                fc += 1
        except AttributeError:
            continue
    return fc


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

    with alive_bar(
        fcount(ccdata),
        title=" - Storing files  ",
        bar="filling",
        spinner="dots",
        stats_end=False,
    ) as bar:
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
                        bar()
                    except Exception as e:
                        logger.error(f"Failed to write s3:{db_bucket}/{keydate}")
                        logger.exception(e)
                        return False
    return len(files.items())


def download(url):
    logger.debug(f"Download: {url}")
    filename = url.split("/")[-1]
    try:
        with tempfile.NamedTemporaryFile() as tmp:
            logger.debug(f"Temp file: {tmp.name}")
            with requests.get(url, stream=True) as r:
                chunk = 8192
                r.raise_for_status()
                logger.debug(f"Got HTTP code {r.status_code}")
                totalsize = int(r.headers.get("content-length"))
                with alive_bar(
                    total=int(math.ceil(totalsize / chunk)) + 10,
                    title=" - Get raw data   ",
                    bar="filling",
                    spinner="dots",
                    monitor=False,
                    stats_end=False,
                ) as bar:
                    with open(tmp.name, "wb") as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
                            bar()
                        if filename.endswith(".gz"):
                            logger.debug("Uncompress gzip file")
                            with gzip.open(tmp.name, "rt") as f:
                                bar(10)
                                return f.read().splitlines()
                        elif filename.endswith(".bz2"):
                            logger.debug("Uncompress bzip2 file")
                            with bz2.open(tmp.name, "rt") as f:
                                bar(10)
                                return f.read().splitlines()
                        else:
                            logger.debug("No compression used")
                            with open(tmp.name, "rt") as f:
                                bar(10)
                                return f.read().splitlines()
    except Exception as e:
        logger.exception(e)
        logger.error(f"Failed to download: {url}")
        return False


if __name__ == "__main__":
    logzero.loglevel(logging.getLevelName(LOGLEVEL))

    # Get input date
    if len(sys.argv) != 4:
        exit("Usage: history.py <yyyy> <mm> <dd>")
    y = sys.argv[1]
    m = sys.argv[2]
    d = sys.argv[3]

    # Validate date
    try:
        dt = datetime(int(y), int(m), int(d))
    except ValueError as e:
        exit(f"Invalid date: {str(e)}")

    if dt >= datetime.combine(datetime.today(), datetime.min.time()):
        exit("Invalid date: can't be in the present or future!")

    # AWS S3 reference
    s3 = boto3.resource(service_name="s3", region_name=REGION)

    logger.info(f"Historic import of RIR data for {y}-{m}-{d}")
    for func, src in sources.items():
        if func == "ripe":
            print(f"{func.upper()}")
            url = globals()[func](src, dt)
            data = download(url)
            if data:
                ccdata = parser(data)
                dbstore(ccdata, BUCKET, s3)

    logger.info("All done!")
