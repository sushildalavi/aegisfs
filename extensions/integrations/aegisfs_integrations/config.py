"""Configuration shared by every integration.

Values come from, in order of precedence: a command-line flag, an environment variable, a JSON
configuration file, and finally a built-in default. The file has one object per area:

    {
      "aegisfs":   {"bin": "aegisfs", "entry": "127.0.0.1:9100"},
      "cassandra": {"contact_points": ["127.0.0.1"], "port": 9042},
      "hdfs":      {"namenode_url": "https://namenode:9871"}
    }

Environment variables are named AEGISFS_INT_<AREA>_<KEY> (for example
AEGISFS_INT_CASSANDRA_CONTACT_POINTS). Lists are comma separated. Secrets are only ever read from
files named in the configuration, never from the configuration itself.
"""
import json
import os

CONFIG_ENV = "AEGISFS_INTEGRATIONS_CONFIG"

_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off", ""}


class ConfigError(Exception):
    """The configuration is missing something or contains an unusable value."""


class Config:
    def __init__(self, data=None, environ=None):
        self.data = data or {}
        self.environ = os.environ if environ is None else environ

    @classmethod
    def load(cls, path=None, environ=None):
        env = os.environ if environ is None else environ
        path = path or env.get(CONFIG_ENV)
        data = {}
        if path:
            try:
                with open(path, "r", encoding="utf-8") as handle:
                    data = json.load(handle)
            except OSError as exc:
                raise ConfigError("cannot read configuration file %s: %s" % (path, exc))
            except ValueError as exc:
                raise ConfigError("configuration file %s is not valid JSON: %s" % (path, exc))
            if not isinstance(data, dict):
                raise ConfigError("configuration file %s must contain a JSON object" % path)
        return cls(data, env)

    def section(self, area):
        value = self.data.get(area, {})
        if not isinstance(value, dict):
            raise ConfigError("configuration section %r must be an object" % area)
        return value

    def get(self, area, key, default=None, override=None):
        """Look up one setting. `override` is a value already taken from a command-line flag."""
        if override is not None:
            return override
        env_name = "AEGISFS_INT_%s_%s" % (area.upper(), key.upper())
        if env_name in self.environ:
            return self.environ[env_name]
        section = self.section(area)
        if key in section:
            return section[key]
        return default

    def get_bool(self, area, key, default=False, override=None):
        value = self.get(area, key, default, override)
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in _TRUE:
            return True
        if text in _FALSE:
            return False
        raise ConfigError("%s.%s must be true or false, got %r" % (area, key, value))

    def get_int(self, area, key, default=0, override=None):
        value = self.get(area, key, default, override)
        try:
            return int(value)
        except (TypeError, ValueError):
            raise ConfigError("%s.%s must be a whole number, got %r" % (area, key, value))

    def get_list(self, area, key, default=None, override=None):
        value = self.get(area, key, default, override)
        if value is None:
            return []
        if isinstance(value, (list, tuple)):
            return [str(v) for v in value]
        return [part.strip() for part in str(value).split(",") if part.strip()]

    def read_secret(self, area, key):
        """Read a secret from the file named by <key>_file. Returns None if none is configured."""
        path = self.get(area, key + "_file")
        if not path:
            return None
        try:
            with open(path, "r", encoding="utf-8") as handle:
                return handle.read().strip()
        except OSError as exc:
            raise ConfigError("cannot read %s.%s_file (%s): %s" % (area, key, path, exc))


def redact(text, secrets):
    """Replace any known secret in `text` so it cannot reach a log or an error message."""
    for secret in secrets:
        if secret:
            text = text.replace(secret, "***")
    return text
