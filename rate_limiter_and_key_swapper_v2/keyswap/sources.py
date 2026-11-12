"""Where keys come from. All of these are optional; use any mix.

    gw.add_key("sk-...")                          # straight from your own code / secret manager
    gw.add_keys_from_env("GEMINI_KEYS")           # env var (comma separated) and/or a .env file
    gw.add_keys_from_file("keys.txt")             # one key per line
    POST /proxy/keys                              # at runtime, when using the HTTP proxy

The .env reader is a tiny built-in parser, so beginners need no extra package. A real
environment variable always wins over the same name in the .env file.
"""

from __future__ import annotations

import os
import re
from typing import Mapping, Optional


def parse_dotenv(text: str) -> dict[str, str]:
    """Parse .env text: KEY=value, `export KEY=value`, quotes, # comments, blank lines."""
    values: dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].lstrip()
        name, sep, value = line.partition("=")
        name, value = name.strip(), value.strip()
        if not sep or not name:
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        else:
            value = re.split(r"\s+#", value, maxsplit=1)[0].strip()  # inline comment
        values[name] = value
    return values


def read_dotenv(path: str = ".env") -> dict[str, str]:
    """Read a .env file; a missing or unreadable file just means 'no values'."""
    try:
        with open(path, "r", encoding="utf-8-sig") as fh:
            return parse_dotenv(fh.read())
    except OSError:
        return {}


def _split_keys(value: str) -> list[str]:
    return [k for k in re.split(r"[,\s]+", value.strip()) if k]


def keys_from_env(
    name: str = "API_KEYS",
    *,
    dotenv_path: Optional[str] = None,
    environ: Optional[Mapping[str, str]] = None,
) -> list[str]:
    """Collect keys from the environment (and optionally a .env file).

    Understands both styles:
        API_KEYS=key1,key2,key3
        API_KEYS_1=key1   API_KEYS_2=key2   ...
    Real environment variables override the .env file. Duplicates are dropped, order kept.
    """
    env: dict[str, str] = dict(read_dotenv(dotenv_path)) if dotenv_path else {}
    env.update(environ if environ is not None else os.environ)

    found: list[str] = []
    if env.get(name):
        found.extend(_split_keys(env[name]))
    numbered = sorted(
        (int(m.group(1)), var) for var in env if (m := re.fullmatch(re.escape(name) + r"_(\d+)", var))
    )
    for _, var in numbered:
        found.extend(_split_keys(env[var]))
    return list(dict.fromkeys(found))


def keys_from_file(path: str) -> list[str]:
    """One key per line; blank lines and lines starting with # are ignored."""
    with open(path, "r", encoding="utf-8-sig") as fh:
        lines = [ln.strip() for ln in fh]
    return list(dict.fromkeys(ln for ln in lines if ln and not ln.startswith("#")))
