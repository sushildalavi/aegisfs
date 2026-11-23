import io
import os
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "extensions", "integrations"))
sys.path.insert(0, ROOT)

from aegisfsIntegrations import cli  # noqa: E402


class CliTest(unittest.TestCase):
    def run_cli(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            try:
                code = cli.main(list(argv))
            except SystemExit as exc:
                code = exc.code
        return code, out.getvalue(), err.getvalue()

    def test_no_arguments_prints_help_and_exits_with_usage(self):
        code, out, _ = self.run_cli()
        self.assertEqual(code, cli.EXIT_USAGE)
        self.assertIn("aegisfs-integrations", out)

    def test_help_lists_both_areas(self):
        code, out, _ = self.run_cli("--help")
        self.assertEqual(code, 0)
        self.assertIn("cassandra", out)
        self.assertIn("hdfs", out)

    def test_bad_config_file_is_a_usage_error(self):
        code, _, err = self.run_cli("--config", "/nonexistent.json", "cassandra", "status")
        self.assertEqual(code, cli.EXIT_USAGE)
        self.assertIn("configuration error", err)

    def test_emit_json_and_text(self):
        class Args:
            json = True
        out = io.StringIO()
        with redirect_stdout(out):
            cli.emit(Args, {"b": 1, "a": 2})
        self.assertIn('"a": 2', out.getvalue())
        Args.json = False
        out = io.StringIO()
        with redirect_stdout(out):
            cli.emit(Args, {"b": 1, "a": 2})
        self.assertEqual(out.getvalue().split(), ["a=2", "b=1"])


if __name__ == "__main__":
    unittest.main()
