#!/bin/bash
#
# Install CC2ASN to local system

if [ "$UID" -ne 0 ]; then 
    echo "Need root privileges (e.g. sudo ./install.sh)"
    exit
fi

# Install CC2ASN configuration file
cp cc2asn.conf /etc/default/cc2asn

# Install Upstart configuration
cp cc2asn.upstart /etc/init/cc2asn.conf
initctl reload-configuration

# Install downloader and update config path
cp RIR-downloader.sh /usr/local/sbin/
sed -i 's/cc2asn.conf/\/etc\/default\/cc2asn/' /usr/local/sbin/RIR-downloader.sh 

# Install parser and update config path
cp SEF-parser.py /usr/local/sbin/
sed -i 's/cc2asn.conf/\/etc\/default\/cc2asn/' /usr/local/sbin/SEF-parser.py

# Install server
cp cc2asn-server.py /usr/local/sbin/

echo "Installation of CC2ASN is complete"

