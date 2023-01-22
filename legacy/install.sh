#!/bin/bash
#
# Install CC2ASN to local system

if [ "$UID" -ne 0 ]; then 
    echo "Need root privileges (e.g. sudo ./install.sh)"
    exit
fi

# Install CC2ASN configuration file
DEST="/etc/default/cc2asn"
if [ -e $DEST ]; then
    diff -q cc2asn.conf $DEST >/dev/null
    if [ $? != 0 ]; then
        printf "%`tput cols`s"|tr ' ' '-'
        echo "diff cc2asn.conf $DEST"
        diff cc2asn.conf $DEST
        printf "%`tput cols`s"|tr ' ' '-'
        echo "Installation script want to overwrite $DEST."
        echo "Any local customizations you have done will be lost."
        read -p "Are you sure you want to do this? [y/N]: " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            cp cc2asn.conf $DEST
        fi
    fi
else
    cp cc2asn.conf $DEST
fi

# Install Upstart configuration
DEST="/etc/init/cc2asn.conf"
if [ -e $DEST ]; then
    diff -q cc2asn.upstart $DEST >/dev/null
    if [ $? != 0 ]; then
        printf "%`tput cols`s"|tr ' ' '-'
        echo "diff cc2asn.upstart $DEST"
        diff cc2asn.upstart $DEST
        printf "%`tput cols`s"|tr ' ' '-'
        echo "Installation script want to overwrite $DEST."
        echo "Any local customizations you have done will be lost."
        read -p "Are you sure you want to do this? [y/N]: " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            cp cc2asn.upstart /etc/init/cc2asn.conf
            initctl reload-configuration
        fi
    fi
else
    cp cc2asn.upstart /etc/init/cc2asn.conf
    initctl reload-configuration
fi

# Install downloader and update config path
cp RIR-downloader.sh /usr/local/sbin/
sed -i 's/cc2asn.conf/\/etc\/default\/cc2asn/' /usr/local/sbin/RIR-downloader.sh 

# Install parser and update config path
cp SEF-parser.py /usr/local/sbin/
sed -i 's/cc2asn.conf/\/etc\/default\/cc2asn/' /usr/local/sbin/SEF-parser.py

# Install server
cp cc2asn-server.py /usr/local/sbin/

echo "Installation of CC2ASN is complete"

