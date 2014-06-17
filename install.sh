#!/bin/bash
#
# Install CC2ASN to local system

if [ "$UID" -ne 0 ]; then 
    echo "Need root privileges (e.g. sudo ./install.sh)"
    exit
fi

cp cc2asn.conf /etc/default/cc2asn

cp cc2asn.upstart /etc/init/cc2asn.conf

cp RIR-downloader.sh /usr/local/sbin/
sed -i 's/cc2asn.conf/\/etc\/default\/cc2asn/' /usr/local/sbin/RIR-downloader.sh 

cp SEF-parser.py /usr/local/sbin/
sed -i 's/cc2asn.conf/\/etc\/default\/cc2asn/' /usr/local/sbin/SEF-parser.py

cp cc2asn-server.py /usr/local/sbin/

echo "Installation of CC2ASN is complete"

