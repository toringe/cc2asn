#!/bin/bash
################################################################################
#
# RIR Downloader
# ---------------
#
# Download each of the RIRs delegations as defined in the configuration file to
# the defined data directory. There is no need to run this script more than once
# a day, since the delegated files from the registrars are only updated daily.
#
# Exit codes:
#              0 - OK
#              1 - Failed to create DATADIR
#              2 - Failed to write to DATADIR
#              3 - Failed to download delegated file
#              4 - Failed to download checksum
#              5 - Failed to verify checksum
#
# Author: Tor Inge Skaar
#
################################################################################

# Read configuration
source "cc2asn.conf"

# Create DATADIR if it doesn't exists
if [ ! -d $DATADIR ]; then
	mkdir -p $DATADIR 
    if [ $? != 0 ]; then
        exit 1
    fi
fi
if [ ! -w $DATADIR ]; then
    echo "Unable to write to $DATADIR" >&2
    exit 2
fi
curdir=`pwd`
cd $DATADIR

ecode=0
i=1
varname="RIR"$i
while [ -n "${!varname}" ]
do
    url=${!varname}
	rirfile=`basename $url`
    
    # Create header
    rir=`echo $rirfile | cut -d- -f2 | tr [:lower:] [:upper:]`
    head="--| $rir |--"
    headlen=${#head}
    twidth=`tput cols`
    echo -n $head; yes - | head -$(( $twidth - $headlen )) | paste -s -d '' -

    # Download delegated file
	echo "Downloading $url"
	wget -O $rirfile "$url"
	if [ $? != 0 ]; then
        echo "[ERROR] Wget failed to download $rirfile" >&2
        rm $rirfile
        ecode=3
	else
        if [ $MD5 ]; then
            # Download checksum
            url=$url".md5"
            md5file=$rirfile".md5"
	        wget -O $md5file "$url"
	        if [ $? != 0 ]; then
                echo "[ERROR] Wget failed to download $md5file" >&2
                rm $md5file
                ecode=4
            else
                # Latest file is only symlink, so md5 file contains actual name
                sed -i 's/-[0-9]\{8\}/-latest/' $md5file

                # Verify checksum
                md5sum -w -c $md5file
                if [ $? != 0 ]; then
                    ecode=5
                fi
            fi
        fi
	fi
    echo
	let "i+=1"
	varname="RIR"$i
done
cd $curdir
exit $ecode
