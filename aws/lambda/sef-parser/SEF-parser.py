#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Parse RIR SEF files and output country spesific data files.
Spec: ftp.ripe.net/pub/stats/ripencc/RIR-Statistics-Exchange-Format.txt

Author: Tor Inge Skaar

'''
# Core modules
from __future__ import print_function
import os
import re
import sys
import math
import boto3
import urllib
#import configobj
import collections

# Third-party modules
#import natsort

# Store parsed data in an ordered dictionary
ccdata = collections.OrderedDict()

#obj = s3.Object(bucket_name='cc2asn-data', key='delegated-lacnic-extended-latest')

# Create a S3 resource object
s3 = boto3.resource('s3')

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    filename = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    if filename.endswith('latest'):
        try:
            obj = s3.Object(bucket_name=bucket, key=filename)
            sef_parse(obj.get()['Body'].read().splitlines())
        except Exception as e:
            print(e)
            raise e

def sef_parse(lines):
    for line in lines:
        # Remove all whitespace trash
        line = line.strip()

        # Skip all comments
        if line.startswith('#'):
            continue

        # Skip header
        if line[0].isdigit():
            continue

        # Skip summary
        if line.endswith('summary'):
            continue

        # Skip not allocated records
        if (line.rstrip('|').endswith('available') or
            line.rstrip('|').endswith('reserved')):
            continue

        # Extract records
        # registry|cc|type|start|value|date|status[|extensions...]
        elements = map(lambda x: x.upper(), line.split('|'))
        cc = elements[1]
        iptype = elements[2]
        start = str(elements[3])
        value = int(elements[4])

        # Process prefixes and ASNs
        if iptype == 'IPV4' or iptype == 'IPV6':
            if ':' not in start:
                value = int(math.ceil(32 - math.log(value, 2)))
                record = start + '/' + str(value)
            elif iptype == 'ASN':
                record = 'AS' + start
            else:
                print("WARNING: Undefined record type: {}".format(iptype),
                      file=sys.stderr)

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

        print(ccdata)
        exit()

# Just for testing
if __name__ == "__main__":
    event = {
  "Records": [
    {
      "eventVersion": "2.0",
      "eventTime": "1970-01-01T00:00:00.000Z",
      "requestParameters": {
        "sourceIPAddress": "127.0.0.1"
      },
      "s3": {
        "configurationId": "testConfigRule",
        "object": {
          "eTag": "0123456789abcdef0123456789abcdef",
          "sequencer": "0A1B2C3D4E5F678901",
          "key": "delegated-lacnic-extended-latest",
          "size": 1024
        },
        "bucket": {
          "arn": "arn:aws:s3:::mybucket",
          "name": "cc2asn-data",
          "ownerIdentity": {
            "principalId": "EXAMPLE"
          }
        },
        "s3SchemaVersion": "1.0"
      },
      "responseElements": {
        "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH",
        "x-amz-request-id": "EXAMPLE123456789"
      },
      "awsRegion": "us-east-1",
      "eventName": "ObjectCreated:Put",
      "userIdentity": {
        "principalId": "EXAMPLE"
      },
      "eventSource": "aws:s3"
    }
  ]
}

lambda_handler(event, None)


# Parse each RIR SEF file
# for seffile in os.listdir(config.get('DATADIR')):
#     if seffile.endswith("latest"):
#         with open(os.path.join(config.get('DATADIR'), seffile)) as f:
#             for line in f:
#
#                 # Remove all whitespace trash
#                 line = line.strip()
#
#                 # Skip all comments
#                 if line.startswith('#'):
#                     continue
#
#                 # Skip header
#                 if line[0].isdigit():
#                     continue
#
#                 # Skip summary
#                 if line.endswith('summary'):
#                     continue
#
#                 # Skip not allocated records
#                 if (line.rstrip('|').endswith('available') or
#                    line.rstrip('|').endswith('reserved')):
#                     continue
#
#                 # Extract records
#                 # registry|cc|type|start|value|date|status[|extensions...]
#                 elements = map(lambda x: x.upper(), line.split('|'))
#                 cc = elements[1]
#                 iptype = elements[2]
#                 start = str(elements[3])
#                 value = int(elements[4])
#
#                 # Process prefixes and ASNs
#                 if iptype == 'IPV4' or iptype == 'IPV6':
#                     if ':' not in start:
#                         value = int(math.ceil(32 - math.log(value, 2)))
#                     record = start + '/' + str(value)
#                 elif iptype == 'ASN':
#                     record = 'AS' + start
#                 else:
#                     print("WARNING: Undefined record type: {}".format(iptype),
#                           file=sys.stderr)
#
#                 # Structurize records
#                 typedata = {}
#                 if cc in ccdata:
#                     typedata = ccdata[cc]
#                     if iptype in typedata:
#                         data = typedata[iptype]
#                         data.append(record)
#                         typedata[iptype] = data
#                         ccdata[cc] = typedata
#                     else:
#                         typedata[iptype] = [record]
#                         ccdata[cc] = typedata
#                 else:
#                     typedata[iptype] = [record]
#                     ccdata[cc] = typedata
#
# # Try to create directory for datafiles
# dbdir = config.get('DBDIR')
# if not os.path.exists(dbdir):
#     try:
#         os.makedirs(dbdir)
#     except:
#         print("ERROR: Unable to create directory: {}".format(dbdir),
#               file=sys.stderr)
#         exit(1)
#
# # Output data to country spesific files
# dbdir = config.get('DBDIR')
# for cc in ccdata:
#     alldata = []
#     typedata = ccdata[cc]
#     types = sorted(typedata.keys())
#     for iptype in types:
#         filepath = os.path.join(dbdir, cc + '_' + iptype)
#         if iptype == "IPV6":
#             data = sorted(typedata[iptype])
#         else:
#             data = natsort.natsort(typedata[iptype])
#         alldata += data
#
#         with open(filepath, 'w+') as f:
#             for item in data:
#                 print(item, file=f)
#
#     # Create a combined file as well
#     filepath = os.path.join(dbdir, cc + '_ALL')
#     with open(filepath, 'w+') as f:
#         for item in alldata:
#             print(item, file=f)
