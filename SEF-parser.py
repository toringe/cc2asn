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
import configobj
import collections

# Third-party modules
import natsort

# Load configuration
config = configobj.ConfigObj('cc2asn.conf')

# Store parsed data in an ordered dictionary
ccdata = collections.OrderedDict()

# Parse each RIR SEF file
for seffile in os.listdir(config.get('DATADIR')):
    if seffile.endswith("latest"):
        with open(os.path.join(config.get('DATADIR'), seffile)) as f:
            for line in f:

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

# Try to create directory for datafiles
dbdir = config.get('DBDIR')
if not os.path.exists(dbdir):
    try:
        os.makedirs(dbdir)
    except:
        print("ERROR: Unable to create directory: {}".format(dbdir),
              file=sys.stderr)
        exit(1)

# Output data to country spesific files
dbdir = config.get('DBDIR')
for cc in ccdata:
    alldata = []
    typedata = ccdata[cc]
    types = sorted(typedata.keys())
    for iptype in types:
        filepath = os.path.join(dbdir, cc + '_' + iptype)
        if iptype == "IPV6":
            data = sorted(typedata[iptype])
        else:
            data = natsort.natsorted(typedata[iptype])
        alldata += data

        with open(filepath, 'w+') as f:
            for item in data:
                print(item, file=f)

    # Create a combined file as well
    filepath = os.path.join(dbdir, cc + '_ALL')
    with open(filepath, 'w+') as f:
        for item in alldata:
            print(item, file=f)
