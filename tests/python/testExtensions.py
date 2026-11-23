#!/usr/bin/env python3
"""Small, dependency-free tests for the Python extension surfaces.

These tests deliberately mock the CLI subprocess.  They validate gateway/exporter
protocol behavior without requiring a running node, Docker, or localhost sockets.
"""
import importlib.util
import pathlib
import subprocess
import unittest
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[2]


def load_module(name, relative_path):
    path = ROOT / relative_path
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


gateway = load_module("gateway_test", "extensions/gateway/gateway.py")
exporter = load_module("aegisfs_exporter_test", "extensions/metrics/exporter.py")


class FakeHeaders:
    def __init__(self, values):
        self.values = values

    def get(self, key, default=""):
        return self.values.get(key, default)


class ExtensionTest(unittest.TestCase):
    def test_gateway_cli_parses_fields_and_stderr(self):
        completed = subprocess.CompletedProcess(
            ["aegisfs"], 0, "status=OK\ncommitted=3\n", ""
        )
        with mock.patch.object(gateway.subprocess, "run", return_value=completed) as run:
            code, fields, err = gateway.cli("health")
        self.assertEqual(code, 0)
        self.assertEqual(fields, {"status": "OK", "committed": "3"})
        self.assertEqual(err, "")
        run.assert_called_once()

    def test_gateway_authorization_requires_exact_bearer_token(self):
        with mock.patch.object(gateway, "TOKEN", "secret"):
            handler = object.__new__(gateway.Handler)
            handler.headers = FakeHeaders({"Authorization": "Bearer secret"})
            self.assertTrue(handler._authorized())
            handler.headers = FakeHeaders({"Authorization": "secret"})
            self.assertFalse(handler._authorized())
            handler.headers = FakeHeaders({})
            self.assertFalse(handler._authorized())

    def test_gateway_name_decodes_nested_path(self):
        handler = object.__new__(gateway.Handler)
        handler.path = "/files/team%2Freport/data.txt?ignored=1"
        self.assertEqual(handler._name(), "team/report/data.txt")

    def test_exporter_emits_only_numeric_metric_fields(self):
        completed = subprocess.CompletedProcess(
            ["aegisfs"],
            0,
            "counter.op_create=4\n"
            "lat.create.p50_ms=12.5\n"
            "counter.bad=not-a-number\n"
            "status=OK\n",
            "",
        )
        with mock.patch.object(exporter.subprocess, "run", return_value=completed):
            body = exporter.scrape().decode()
        self.assertIn("aegisfs_counter_op_create 4", body)
        self.assertIn("aegisfs_lat_create_p50_ms 12.5", body)
        self.assertNotIn("aegisfs_counter_bad", body)
        self.assertNotIn("status=OK", body)


if __name__ == "__main__":
    unittest.main()
