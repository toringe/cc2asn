#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
CC2ASN Server

Basic query-response server listen on a tcp-port (default 43) for
incoming requests. The only valid and required request is an
ISO-3166-1 alpha-2 country code (e.g. NO for Norway). The server
will respond back with the list of registered AS-numbers.
Optionally a record type (IPv4, IPv6 or ALL) may be specified to
get prefixes instead of ASNs, or to get everything that is
registered for this country. Logs all system messages and client
queries to local syslog.


Author: Tor Inge Skaar

'''

# Core modules
import os
import re
import sys
import pwd
import grp
import signal
import argparse
import datetime
import configobj
import threading
import SocketServer
import logging
from logging.handlers import SysLogHandler


# Each time a client connect, a new instance of this class is created.
class RequestHandler(SocketServer.BaseRequestHandler):

    # Handle the incomming request
    def handle(self):

        # Receive 8 bytes of data, and convert to uppercase
        sockdata = self.request.recv(8).strip().upper()

        # Client IP
        client = self.client_address[0]

        # First match cc search without rectype
        ccmatch = re.match('^([A-Z]{2})$', sockdata)
        if ccmatch is not None:
            # Defaulting to ASN
            rectype = 'ASN'
            cc = ccmatch.group(1)
        else:
            # Check if record type is defined
            recmatch = re.match('^(ALL|ASN|IPV4|IPV6) ([A-Z]{2})$', sockdata)
            if recmatch is not None:
                rectype = recmatch.group(1)
                cc = recmatch.group(2)
            else:
                self.server.logger.error('Invalid query from ' + client +
                                         ': ' + str(sockdata))
                return

        # Construct path to file and send the contents to client
        datafile = cc + '_' + rectype
        datapath = os.path.join(self.server.config.get('DBDIR'), datafile)
        if os.path.isfile(datapath) and os.access(datapath, os.R_OK):
            with open(datapath, 'r') as data:
                self.request.send(data.read())
                self.server.logger.info('Query: ' + client + ' ' + rectype +
                                        ' ' + cc)
        else:
            self.server.logger.warning('Client ' + client +
                                       ' queried for missing file: '+datapath)
        return


# Change execution UID and GID
def drop_privileges(uid_name, gid_name):

    # We're not root, so everythings fine then..
    if os.getuid() != 0:
        return

    # Get the uid/gid from the name
    try:
        running_uid = pwd.getpwnam(uid_name).pw_uid
    except KeyError:
        e = 'Unable to drop privileges. No such user: {}'.format(uid_name)
        logger.critical(e)
        exit(e)
    try:
        running_gid = grp.getgrnam(gid_name).gr_gid
    except KeyError:
        e = 'Unable to drop privileges. No such group: {}'.format(gid_name)
        logger.critical(e)
        exit(e)

    # Remove group privileges
    os.setgroups([])

    # Try setting the new uid/gid
    os.setgid(running_gid)
    os.setuid(running_uid)

    # Ensure a very conservative umask
    old_umask = os.umask(077)


# Run process as a daemon by double forking
def daemonize():
    try:
        pid = os.fork()
        if pid > 0:
            # Exit first parent
            exit()
    except OSError as e:
        exit('Fork #1 failed: {} ({})'.format(e.errno, e.strerror))

    os.chdir('/')
    os.setsid()
    os.umask(0)

    try:
        pid = os.fork()
        if pid > 0:
            # Exit second parent
            exit()
    except OSError as e:
        exit('Fork #2 failed: {} ({})'.format(e.errno, e.strerror))


# Handle user input
def parse_input():
    parser = argparse.ArgumentParser(description='CC2ASN Server')
    parser.add_argument('-c', dest='confpath', help='Path to config file')
    parser.add_argument('-D', dest='daemon', action='store_true',
                        help='Daemonize server')
    parser.add_argument('-V', action='version', version='CC2ASN Server v.1')
    args = parser.parse_known_args()[0]

    if args.confpath is None:
        args.confpath = '/etc/default/cc2asn'
        logger.info('No config file specified. Using {}'.format(args.confpath))

    return args


# Create signal handlers for the usual interrupts
def signal_handling():
    logger.info('Installing signal handlers')
    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)
    signal.signal(signal.SIGQUIT, cleanup)


# Cleanup process in separate thread
def cleanup(signal, frame):
    logger.warning('Interrupted by {}'.format(signalname[signal]))
    t = threading.Thread(target=shutdown_handler, args=(shutdown_event,))
    t.start()


# Proper shutdown of socketserver
def shutdown_handler(event):
    logger.info('Shutting down server')
    try:
        server.shutdown()
        logger.info('Successful shutdown')
    except:
        logger.warning('Failed to cleanly shutdown server')
    event.set()
    return


# Main execution
if __name__ == '__main__':

    # Log to local syslog
    logger = logging.getLogger('CC2ASN')
    logger.setLevel(logging.INFO)
    syslog = SysLogHandler(address='/dev/log')
    formatter = logging.Formatter('%(name)s: <%(levelname)s> -  %(message)s')
    syslog.setFormatter(formatter)
    logger.addHandler(syslog)

    # Create signal name lookup
    signalname = dict((k, v) for v, k in
                      signal.__dict__.iteritems() if v.startswith('SIG'))
    signal_handling()

    # Handle user input
    args = parse_input()

    # Load configuration
    if os.access(args.confpath, os.R_OK):
        config = configobj.ConfigObj(args.confpath)
    else:
        exit('Failed to read configuration file: {}'.format(args.confpath))

    # Allow server to reuse a socket immediately after socket closure
    SocketServer.TCPServer.allow_reuse_address = True

    # Kill server thread when main thread terminates
    SocketServer.TCPServer.daemon_threads = True

    # Create a threaded TCP server, spawning separate threats for each client
    listen = int(config.get('PORT'))
    try:
        server = SocketServer.ThreadingTCPServer(('', listen), RequestHandler)
        (ip, port) = server.server_address
        logger.info('Server bound to {}:{}'.format(ip, port))
    except IOError as e:
        if e.errno == 13:
            errmsg = 'Premission denied to bind port {}'.format(listen)
        else:
            errmsg = e.strerror
        logger.critical(errmsg)
        exit(errmsg)

    # Share local config and syslogger with server
    server.config = config
    server.logger = logger

    if args.daemon is True:
        # Drop root privileges
        user = config.get('RUNUSER')
        group = config.get('RUNGROUP')
        drop_privileges(user, group)
        logger.info('Privileges dropped to {}:{}'.format(user, group))
        # Daemonize
        daemonize()
        logger.info('Daemonized (pid {})'.format(os.getpid()))
    else:
        logger.info('Server running in foreground (pid {})'
                    .format(os.getpid()))

    # Create an event for the shutdown process to set
    shutdown_event = threading.Event()

    # Server must handle requests indefinitely until a shutdown is requested
    server.serve_forever()

    # Main thread will wait for shutdown to finish
    shutdown_event.wait()