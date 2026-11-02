"""Configuration parsing (bridge/config) and serve() wiring tests.

The conf file is INI (stdlib configparser) in the legacy server's spirit;
keys are PORT, API_BASE, BRIDGE_SECRET, QUERYLOG.
"""

import os
import shutil
import tempfile
import unittest

from server import load_config, parse_query, serve


CONF = """\
[bridge]
PORT = 43
API_BASE = https://api.example.on.aws
BRIDGE_SECRET = s3cr3t
QUERYLOG = /var/log/cc2asn/query.log
"""


class LoadConfigTest(unittest.TestCase):
    def setUp(self):
        tmpdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmpdir, ignore_errors=True)
        self.conf_path = os.path.join(tmpdir, "cc2asn-bridge.conf")

    def write(self, text):
        with open(self.conf_path, "w") as fh:
            fh.write(text)
        return self.conf_path

    def test_reads_all_keys(self):
        config = load_config(self.write(CONF))
        self.assertEqual(config["PORT"], 43)
        self.assertEqual(config["API_BASE"], "https://api.example.on.aws")
        self.assertEqual(config["BRIDGE_SECRET"], "s3cr3t")
        self.assertEqual(config["QUERYLOG"], "/var/log/cc2asn/query.log")

    def test_missing_config_is_an_error_naming_the_file(self):
        with self.assertRaises(SystemExit) as caught:
            load_config("/no/such/path.conf")
        self.assertIn("/no/such/path.conf", str(caught.exception))

    def test_missing_key_is_an_error_naming_the_key(self):
        path = self.write(
            "[bridge]\nPORT = 43\nAPI_BASE = https://x.example\n")
        with self.assertRaises(SystemExit) as caught:
            load_config(path)
        self.assertIn("BRIDGE_SECRET", str(caught.exception))

    def test_non_numeric_port_is_an_error(self):
        path = self.write(CONF.replace("PORT = 43", "PORT = forty-three"))
        with self.assertRaises(SystemExit) as caught:
            load_config(path)
        self.assertIn("PORT", str(caught.exception))


class ServeTest(unittest.TestCase):
    """serve() binds the configured port and passes the config through."""

    def test_serve_builds_the_listening_server(self):
        tmpdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmpdir, ignore_errors=True)
        log_path = os.path.join(tmpdir, "q.log")
        config = {
            "PORT": 0,  # ephemeral: real service runs 43
            "API_BASE": "https://api.example.on.aws",
            "BRIDGE_SECRET": "s3cr3t",
            "QUERYLOG": log_path,
        }
        server = serve(config)
        try:
            self.assertEqual(
                server.api_base, "https://api.example.on.aws")
            self.assertEqual(server.secret, "s3cr3t")
            self.assertTrue(server.querylog.handlers)
            host, port = server.server_address
            self.assertEqual(host, "0.0.0.0")
            self.assertGreater(port, 0)
        finally:
            server.server_close()


if __name__ == "__main__":
    unittest.main()
