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
import errno
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
        try:
            sockdata = self.request.recv(8)
        except IOError as e:
            if e.errno == errno.ECONNRESET:
                self.server.logger.warning('Connection reset by client')
                return
            else:
                raise
        if sockdata is not None:
            sockdata = sockdata.strip().upper()
        else:
            self.server.logger.warning('No client data received')
            return

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
                self.logclient(client, rectype, cc)
        else:
            self.server.logger.warning('Client ' + client +
                                       ' queried for missing file: '+datapath)
        return

    # Log client requests
    def logclient(self, ip, rectype, cc):
        if self.server.clientlog is None:
            # Use syslog
            self.server.logger.info('Query: ' + ip + ' ' + rectype + ' ' + cc)
        else:
            # Use custom log
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log = open(self.server.clientlog, 'a')
            log.write('{} {} {} {}\n'.format(now, ip, rectype, cc))
            log.close()
# End class


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
    parser.add_argument('-d', dest='daemon', action='store_true',
                        help='Daemonize server')
    parser.add_argument('-l', dest='clientlog',
                        help='Log client requests to custom file')
    parser.add_argument('-p', dest='pidfile', help='Path to PID file')
    parser.add_argument('-V', action='version', version='CC2ASN Server v.1')
    args = parser.parse_known_args()[0]

    if args.confpath is None:
        args.confpath = '/etc/default/cc2asn'
        logger.info('No config file specified. Using {}'.format(args.confpath))

    return args


# Create an empty file
def touch(filename, desc):
    if os.path.isfile(filename) is True:
        return
    else:
        try:
            f = open(filename, 'w+')
            f.close()
            logger.info('{}: {}'.format(desc, filename))
        except IOError as e:
            errmsg = e.strerror + ': ' + filename
            logger.critical(errmsg)
            exit(errmsg)


# Create subdirectory for pid file.
# This enables deletion after we drop privileges.
def create_piddir(piddir, user, group):

    # Create directory if needed
    if os.path.exists(piddir) is False:
        try:
            os.mkdir(piddir)
        except OSError as e:
            logger.error('Failed to create directory: {}'.format(piddir))

    # Change owner
    try:
        uid = pwd.getpwnam(user).pw_uid
        gid = grp.getgrnam(group).gr_gid
        os.chown(piddir, uid, gid)
    except OSError as e:
        logger.error('Failed to chown {}'.format(piddir))


# Create PID file and check/set permissions
def create_pidfile(pidfile, pid):

    if os.path.isfile(pidfile) is False:
        try:
            f = open(pidfile, 'w+')
            f.write(str(pid))
            f.close()
            logger.info('PID file created: {}'.format(pidfile))
        except IOError as e:
            logger.error('Failed to create pid file: {}'.format(pidfile))
    else:
        logger.warning('PID file already exists. Stale file?')


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

    # Cleanly shutdown server
    try:
        server.shutdown()
        logger.info('Successful shutdown')
    except Exception as e:
        logger.error('Failed: {}'.format(e.strerror))

    # Remove pid file
    try:
        # was config.get(pidfile)
        pidfile = args.pidfile
        logger.info('Removing PID file: {}'.format(pidfile))
        os.remove(pidfile)
    except OSError as e:
        logger.warning('Failed to remove PID file. {}'.format(e.strerror))

    # Tell thread that shutdown event is complete
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

    # Handle user input
    args = parse_input()

    # Create signal name lookup
    signalname = dict((k, v) for v, k in
                      signal.__dict__.iteritems() if v.startswith('SIG'))
    signal_handling()

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

    # Share variables with server
    server.clientlog = args.clientlog
    server.config = config
    server.logger = logger

    if args.daemon is True:

        # Get settings from config
        user = config.get('RUNUSER')
        group = config.get('RUNGROUP')

        # Set default pid file if not specified
        if args.pidfile is None:
            args.pidfile = '/var/run/cc2asn/cc2asn-server.pid'

        # Create subdirectory for pid file
        create_piddir(os.path.dirname(args.pidfile), user, group)

        # Drop root privileges
        drop_privileges(user, group)
        logger.info('Privileges dropped to {}:{}'.format(user, group))

        # Daemonize
        daemonize()
        pid = os.getpid()
        logger.info('Daemonized (pid {})'.format(pid))

        # Create PID file
        create_pidfile(args.pidfile, pid)

    else:
        logger.info('Server running in foreground (pid {})'
                    .format(os.getpid()))

    # If custom log is set, create it if not exists
    if args.clientlog is not None:
        if os.path.isfile(args.clientlog) is False:
            touch(args.clientlog, 'Client log')
        else:
            if os.access(args.clientlog, os.W_OK) is False:
                errmsg = 'Unable to write to file: {}'.format(args.clientlog)
                logger.critical(errmsg)
                exit(errmsg)

    # Create an event for the shutdown process to set
    shutdown_event = threading.Event()

    # Server must handle requests indefinitely until a shutdown is requested
    server.serve_forever()

    # Main thread will wait for shutdown to finish
    shutdown_event.wait()
