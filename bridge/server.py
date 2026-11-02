#!/usr/bin/env python3
"""CC2ASN whois-web API bridge.

Whois-protocol (TCP 43) front-end for the CC2ASN web API. A behavior-preserving
Python 3 port of legacy/cc2asn-server.py: the query grammar, response shape and
connection lifecycle are identical, but per-country files are no longer read
from local disk — every lookup is fetched from the web API on behalf of the
whois client, and the client's IP travels along in X-Forwarded-For (protected
by the bridge secret) so the API can log and rate-limit per user instead of
per bridge.

Query grammar (unchanged from legacy):
    query := COUNTRY | TYPE COUNTRY
    COUNTRY := ISO-3166-1 alpha-2 (e.g. NO)
    TYPE := ALL | ASN | IPV4 | IPV6   (bare COUNTRY means ASN)
"""

import re
import socketserver
import logging
import logging.handlers
import configparser
import time

from lib import api

# Query validation, lifted verbatim (modulo upper-casing) from legacy/cc2asn-server.py
CC_MATCH = re.compile('^([A-Z]{2})$')
TYPE_CC_MATCH = re.compile('^(ALL|ASN|IPV4|IPV6) ([A-Z]{2})$')

# Error lines sent to the whois client only when the *service* is at fault;
# invalid queries and missing countries stay silent, exactly like legacy.
RATE_LIMIT_LINE = b'Rate limit exceeded\n'
UNAVAILABLE_LINE = b'Service unavailable\n'
LEGACY_RECV_BYTES = 8


def parse_query(data):
    """Parse one whois query -> (rectype, cc), or None when invalid.

    Accepts str or the bytes a client socket would deliver; mirrors the
    legacy server's strip().upper() normalization.
    """
    if data is None:
        return None
    if isinstance(data, bytes):
        data = data.decode('ascii', errors='replace')
    sockdata = data.strip().upper()
    match = CC_MATCH.match(sockdata)
    if match is not None:
        return 'ASN', match.group(1)
    match = TYPE_CC_MATCH.match(sockdata)
    if match is not None:
        return match.group(1), match.group(2)
    return None


class BridgeServer(socketserver.ThreadingTCPServer):
    """One thread per whois connection, structure inherited from legacy.

    allow_reuse_address + daemon_threads mirror legacy/cc2asn-server.py.
    """

    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, addr, api_base, secret, querylog, RequestHandlerClass,
                 bind_and_activate=True):
        self.api_base = api_base
        self.secret = secret
        self.querylog = querylog
        super().__init__(addr, RequestHandlerClass, bind_and_activate)


class BridgeRequestHandler(socketserver.BaseRequestHandler):

    # Each time a client connects, a new instance of this class is created.
    def handle(self):
        # Receive up to the same byte count the legacy server reads
        try:
            sockdata = self.request.recv(LEGACY_RECV_BYTES)
        except OSError:
            return

        client = self.client_address[0]
        query = parse_query(sockdata)
        if query is None:
            self.logclient(client, '-', '-', 'INVALID')
            return

        rectype, cc = query
        status, body = api.get(self.server.api_base, self.server.secret,
                               client, rectype, cc)

        if status == 200:
            self.request.sendall(body)
        elif status == 429:
            self.request.sendall(RATE_LIMIT_LINE)
        elif status == 0 or status >= 500:
            self.request.sendall(UNAVAILABLE_LINE)
        # 404 (and other non-5xx): silence, like the legacy server's
        # missing-file path.

        self.logclient(client, rectype, cc, str(status))

    # Log client requests
    def logclient(self, ip, rectype, cc, status):
        self.server.querylog.info('%s %s %s %s', ip, rectype, cc, status)


def make_query_logger(path, max_bytes=10 * 1024 * 1024, backups=5):
    """File logger: `YYYY-MM-DD HH:MM:SS <ip> <type> <cc> <status>` lines.

    Same column shape as the legacy server's clientlog (plus a status
    column for the API side of the exchange). Timestamps are UTC regardless
    of the host's timezone. Self-rotating: no logrotate dependency.
    """
    querylog = logging.getLogger('cc2asn-bridge.query:' + path)
    querylog.setLevel(logging.INFO)
    handler = logging.handlers.RotatingFileHandler(
        path, maxBytes=max_bytes, backupCount=backups)
    handler.setFormatter(logging.Formatter('%(asctime)s %(message)s',
                                           datefmt='%Y-%m-%d %H:%M:%S'))
    handler.converter = time.gmtime
    querylog.addHandler(handler)
    querylog.propagate = False
    return querylog


def make_server(addr, api_base, secret, querylog=None):
    """Factory: journald-backed query logger when no file logger is given."""
    if querylog is None:
        querylog = logging.getLogger('cc2asn-bridge.query')
        if not querylog.handlers:
            querylog.addHandler(logging.StreamHandler())  # stderr → journald
        querylog.setLevel(logging.INFO)
    return BridgeServer(addr, api_base, secret, querylog, BridgeRequestHandler)


def load_config(path):
    """Read the bridge INI file; abort the process naming the culprit."""
    parser = configparser.ConfigParser()
    parser.optionxform = str  # keep key case (PORT, API_BASE, ...), like the legacy conf
    try:
        with open(path) as fh:
            parser.read_file(fh)
        port = parser.getint('bridge', 'PORT')
        config = {
            'PORT': port,
            'API_BASE': parser.get('bridge', 'API_BASE'),
            'BRIDGE_SECRET': parser.get('bridge', 'BRIDGE_SECRET'),
            'QUERYLOG': parser.get('bridge', 'QUERYLOG'),
        }
    except FileNotFoundError:
        raise SystemExit(f'Cannot open config file: {path}')
    except (configparser.NoOptionError, configparser.NoSectionError,
            configparser.Error) as err:
        raise SystemExit(f'Invalid config file {path}: {err}')
    except ValueError as err:
        # getint() failures: name the key ourselves, int() forgets it
        raise SystemExit(f'Invalid config file {path}: PORT must be an '
                         f'integer ({err})')
    return config


def serve(config):
    """Build the listening bridge server from parsed config."""
    querylog = make_query_logger(config['QUERYLOG'])
    return make_server(
        ('', config['PORT']),
        config['API_BASE'],
        config['BRIDGE_SECRET'],
        querylog=querylog,
    )


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='CC2ASN whois-web API bridge (TCP 43 -> HTTPS)')
    parser.add_argument('--config', default='/etc/cc2asn-bridge.conf',
                        help='path to the bridge config file')
    args = parser.parse_args()

    config = load_config(args.config)
    server = serve(config)
    # systemd owns the lifecycle (User=, Restart=); just run.
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == '__main__':
    main()
