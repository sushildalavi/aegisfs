import json
import os
import sys
import tempfile
import unittest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "extensions", "integrations"))
sys.path.insert(0, ROOT)

from aegisfsIntegrations.config import Config, ConfigError, redact  # noqa: E402


class ConfigTest(unittest.TestCase):
    def write(self, data):
        handle = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
        json.dump(data, handle)
        handle.close()
        self.addCleanup(os.unlink, handle.name)
        return handle.name

    def test_default_is_used_when_nothing_is_set(self):
        self.assertEqual(Config({}, {}).get("hdfs", "user", "someone"), "someone")

    def test_file_value_beats_default(self):
        cfg = Config({"hdfs": {"user": "fromfile"}}, {})
        self.assertEqual(cfg.get("hdfs", "user", "someone"), "fromfile")

    def test_environment_beats_file(self):
        cfg = Config({"hdfs": {"user": "fromfile"}}, {"AEGISFS_INT_HDFS_USER": "fromenv"})
        self.assertEqual(cfg.get("hdfs", "user"), "fromenv")

    def test_flag_beats_environment(self):
        cfg = Config({}, {"AEGISFS_INT_HDFS_USER": "fromenv"})
        self.assertEqual(cfg.get("hdfs", "user", override="fromflag"), "fromflag")

    def test_booleans_accept_common_spellings_and_reject_junk(self):
        for text in ("1", "true", "YES", "on"):
            self.assertTrue(Config({}, {"AEGISFS_INT_X_FLAG": text}).get_bool("x", "flag"))
        for text in ("0", "false", "No", "off", ""):
            self.assertFalse(Config({}, {"AEGISFS_INT_X_FLAG": text}).get_bool("x", "flag", True))
        with self.assertRaises(ConfigError):
            Config({}, {"AEGISFS_INT_X_FLAG": "maybe"}).get_bool("x", "flag")

    def test_integers_and_lists(self):
        cfg = Config({"cassandra": {"contact_points": ["a", "b"]}}, {"AEGISFS_INT_CASSANDRA_PORT": "9042"})
        self.assertEqual(cfg.get_int("cassandra", "port"), 9042)
        self.assertEqual(cfg.get_list("cassandra", "contact_points"), ["a", "b"])
        env = Config({}, {"AEGISFS_INT_CASSANDRA_CONTACT_POINTS": "x, y ,"})
        self.assertEqual(env.get_list("cassandra", "contact_points"), ["x", "y"])
        with self.assertRaises(ConfigError):
            Config({}, {"AEGISFS_INT_CASSANDRA_PORT": "nine"}).get_int("cassandra", "port")

    def test_load_reads_the_file_named_in_the_environment(self):
        path = self.write({"aegisfs": {"entry": "10.0.0.1:9100"}})
        cfg = Config.load(environ={"AEGISFS_INTEGRATIONS_CONFIG": path})
        self.assertEqual(cfg.get("aegisfs", "entry"), "10.0.0.1:9100")

    def test_load_reports_a_missing_or_malformed_file(self):
        with self.assertRaises(ConfigError):
            Config.load("/nonexistent/file.json", {})
        bad = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
        bad.write("{not json")
        bad.close()
        self.addCleanup(os.unlink, bad.name)
        with self.assertRaises(ConfigError):
            Config.load(bad.name, {})

    def test_secrets_come_from_files_and_are_stripped(self):
        secret = tempfile.NamedTemporaryFile("w", delete=False)
        secret.write("hunter2\n")
        secret.close()
        self.addCleanup(os.unlink, secret.name)
        cfg = Config({"cassandra": {"password_file": secret.name}}, {})
        self.assertEqual(cfg.read_secret("cassandra", "password"), "hunter2")
        self.assertIsNone(Config({}, {}).read_secret("cassandra", "password"))
        with self.assertRaises(ConfigError):
            Config({"cassandra": {"password_file": "/nonexistent"}}, {}).read_secret("cassandra", "password")

    def test_redact_removes_secrets_from_text(self):
        self.assertEqual(redact("login hunter2 failed", ["hunter2", None, ""]), "login *** failed")


if __name__ == "__main__":
    unittest.main()
