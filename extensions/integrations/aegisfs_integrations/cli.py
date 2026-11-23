"""Command-line entry point: `aegisfs-integrations <area> <command> ...`.

Each area registers its own sub-commands from its package, so the Cassandra and HDFS code never
need to know about each other. An area whose optional dependency is missing reports that when it is
used and does not stop the other area from working.
"""
import argparse
import importlib
import json
import sys

from . import __version__
from .aegisfs_client import AegisClient, AegisError
from .config import Config, ConfigError

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_USAGE = 2
EXIT_UNAVAILABLE = 3  # the integration's backing service (or its Python driver) is not reachable

AREAS = {
    "cassandra": "aegisfs_integrations.cassandra.commands",
    "hdfs": "aegisfs_integrations.hdfs.commands",
}


class IntegrationUnavailable(Exception):
    """The Cassandra cluster, HDFS cluster or a required Python package could not be used."""


class CommandFailed(Exception):
    """The command ran but did not succeed. `payload` is reported to the caller."""

    def __init__(self, message, payload=None):
        super().__init__(message)
        self.payload = payload or {}


def emit(args, payload, human=None):
    """Print a result as JSON (--json) or as readable text."""
    if getattr(args, "json", False):
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    elif human is not None:
        print(human)
    else:
        for key in sorted(payload):
            print("%s=%s" % (key, payload[key]))


def build_parser():
    parser = argparse.ArgumentParser(
        prog="aegisfs-integrations",
        description="Optional Cassandra metadata index and HDFS import/export for AegisFS.",
        epilog="AegisFS works the same with or without either integration.")
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("--config", help="JSON configuration file (or set AEGISFS_INTEGRATIONS_CONFIG)")
    parser.add_argument("--json", action="store_true", help="print results as JSON")
    parser.add_argument("--entry", help="AegisFS entry node host:port (overrides configuration)")
    areas = parser.add_subparsers(dest="area", metavar="<area>")
    for name, module_path in AREAS.items():
        try:
            module = importlib.import_module(module_path)
        except ImportError as exc:
            # The area's own code is missing. Offer a stub so --help still lists it.
            stub = areas.add_parser(name, help="unavailable: %s" % exc)
            stub.set_defaults(handler=_unavailable(name, exc))
            continue
        module.register(areas)
    return parser


def _unavailable(name, exc):
    def handler(args, cfg, aegis):
        raise IntegrationUnavailable("%s integration cannot be loaded: %s" % (name, exc))
    return handler


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "handler", None):
        parser.print_help()
        return EXIT_USAGE
    try:
        cfg = Config.load(args.config)
        if args.entry:
            cfg.data.setdefault("aegisfs", {})["entry"] = args.entry
        aegis = AegisClient.from_config(cfg)
        return args.handler(args, cfg, aegis) or EXIT_OK
    except ConfigError as exc:
        print("configuration error: %s" % exc, file=sys.stderr)
        return EXIT_USAGE
    except IntegrationUnavailable as exc:
        print("unavailable: %s" % exc, file=sys.stderr)
        return EXIT_UNAVAILABLE
    except CommandFailed as exc:
        if args.json and exc.payload:
            print(json.dumps(exc.payload, indent=2, sort_keys=True, default=str))
        print("failed: %s" % exc, file=sys.stderr)
        return EXIT_FAILED
    except AegisError as exc:
        print("aegisfs: %s" % exc, file=sys.stderr)
        return EXIT_FAILED
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
