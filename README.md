CC2ASN
======

A simple lookup service for AS-numbers and prefixes belonging to any given country in the world. For more information check out the website at [www.cc2asn.com][1].

**Note:** if you're only interested in the CC2ASN data, I suggest you use the service on [www.cc2asn.com][1], either by querying the database or simply download the db.tar.gz package, rather than installing a completely new server. 

System prerequisites
--------------------

    sudo apt-get install python-naturalsort unzip

Installation
------------

    wget https://github.com/toringe/cc2asn/archive/master.zip
    unzip master.zip
    cd cc2asn-master
    sudo ./install.sh

Initializing with data
----------------------

    sudo RIR-downloader.sh
    sudo SEF-parser.py

You can run these scripts as a non-privileged user, provided that you set proper permissions on the `DATADIR` and `DBDIR` directories specified in the configuration file `cc2asn.conf`

Starting the server
-------------------

An Upstart-script is provided, and is installed by the installation script `install.sh`. This script will automatically start and stop the server at startup and shutdown. To start the server manually without doing a complete reboot, do the following:

    start cc2asn

Checking the status of the server

    status cc2asn

If you get `cc2asn start/running, process <pid>` (where pid is the process id of the server on your system), then it's up and running.

Troubleshooting
---------------

Check your local syslog file for any problems with the CC2ASN service. Any log entries marked with `WARNING`, `ERROR` or `CRITICAL` should be dealt with.

    zgrep -iE "cc2asn: <warning|error|critical>" /var/log/syslog*

[1]: http://www.cc2asn.com
                                              
