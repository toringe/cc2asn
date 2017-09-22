'''
Parse RIR SEF files and output country spesific data files.
Spec: ftp.ripe.net/pub/stats/ripencc/RIR-Statistics-Exchange-Format.txt

Author: Tor Inge Skaar

'''
# Core modules
import sys
import math
import boto3
import collections

# Third-party modules
import natsort

# Put parsed data in an ordered dictionary
ccdata = collections.OrderedDict()

# Get a S3 resource object
s3 = boto3.resource('s3')


def lambda_handler(event, context):
    # Process delegation files from separate bucket
    bucket = s3.Bucket('cc2asn-data')
    stats = {}
    for content in bucket.objects.all():
        if content.key.endswith('latest'):
            try:
                # Get the file from S3...
                obj = s3.Object(bucket.name, content.key)
                data = obj.get()['Body'].read().decode('utf-8').splitlines()
                stats[content.key.split('-')[1]] = len(data)
            except Exception as e:
                print(e)
                raise e
            # ...and parse it
            sef_parse(data)

    # Store structured data as individual files on S3
    for cc in ccdata:
        alldata = []
        typedata = ccdata[cc]
        types = sorted(typedata.keys())
        for iptype in types:
            filename = cc + '_' + iptype
            if iptype == "IPV6":
                data = sorted(typedata[iptype])
            else:
                data = natsort.natsorted(typedata[iptype])
            alldata += data

            obj = s3.Object('cc2asn-db', filename)
            obj.put(Body='\n'.join(data))

        # Create a combined file as well
        filename = cc + '_ALL'
        obj = s3.Object('cc2asn-db', filename)
        obj.put(Body='\n'.join(alldata))

    # Return with stats on no of records per RIR
    return 'AFRINIC: {}, APNIC: {}, ARIN: {}, LACNIC: {}, RIPENCC: {}'.format(
            stats['afrinic'], stats['apnic'], stats['arin'], stats['lacnic'],
            stats['ripencc'])


def sef_parse(sefdata):
    for line in sefdata:

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
        # 0       |1 |2   |3    |4
        # registry|cc|type|start|value|date|status[|extensions...]
        elements = list(map(lambda x: x.upper(), line.split('|')))
        cc = elements[1]
        iptype = elements[2]
        start = str(elements[3])
        value = int(elements[4])

        # Process prefixes and ASNs
        record = ''
        if iptype == 'IPV4' or iptype == 'IPV6':
            if ':' not in start:
                value = int(math.ceil(32 - math.log(value, 2)))
            record = start + '/' + str(value)
        elif iptype == 'ASN':
            record = 'AS' + start
        else:
            print("WARNING: Undefined record type: {}".format(iptype))

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
