from __future__ import print_function
from contextlib import closing
from datetime import datetime
from urllib2 import urlopen
from shutil import copyfileobj
import re
import boto3
import hashlib

BUCKET = 'cc2asn-data'
SITE = 'http://ftp.ripe.net/pub/stats/lacnic/'
FILE = 'delegated-lacnic-extended-latest'
URI = SITE + FILE


# Calculate MD5 of file
def calc_md5(filename):
    md5 = hashlib.md5()
    with open(filename, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            md5.update(chunk)
    return md5.hexdigest()


# Handler function for Lambda to initate
def lambda_handler(event, context):
    print('Downloading {}'.format(URI))

    # Get the delegation file
    try:
        with closing(urlopen(URI)) as r:
            with open('/tmp/file', 'wb') as f:
                copyfileobj(r, f)
                print('Download completed')
    except:
        print('Failed to download {}!'.format(URI))

    # Get the md5 checksum, generate it, and compare
    try:
        with closing(urlopen(URI+'.md5')) as r:
            orighash = re.findall(r'[a-fA-F\d]{32}', r.read())
        if not orighash:
            raise
        else:
            orighash = orighash[0]
            print('MD5 ({}) = {}'.format(FILE, orighash))
            dlhash = calc_md5('/tmp/file')
            if dlhash != orighash:
                print('Hash mismatch: {} != {}'.format(dlhash, orighash))
            else:
                print('Hash match: {} == {}'.format(dlhash, orighash))
    except:
        print('Failed to get MD5 file: {}'.format(URI+'.md5'))

    # Upload delegation file to S3
    try:
        s3client = boto3.client('s3')
        s3client.upload_file('/tmp/file', BUCKET, FILE)
        print('Uploaded {} to S3:/{}'.format(FILE, BUCKET))
    except:
        print('Failed to upload {} to cc2asn-data on S3'.format(FILE))
