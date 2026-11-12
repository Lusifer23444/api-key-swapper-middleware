"""Tiny JSON file store so key health survives a restart.

Only health is saved: key id, state, cool-down end (wall-clock time), failure count.
Secrets are never written. Your app registers its keys at startup from its own secret
store, then the saved health is applied to them by id.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import threading

log = logging.getLogger("keyswap")


class JsonStateStore:
    def __init__(self, path: str) -> None:
        self.path = path
        self._lock = threading.Lock()

    def load(self) -> dict:
        try:
            with open(self.path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            return data if isinstance(data, dict) else {}
        except FileNotFoundError:
            return {}
        except (OSError, ValueError):
            log.warning("could not read key state file %s; starting fresh", self.path)
            return {}

    def save(self, state: dict) -> None:
        """Atomic write (temp file + rename) so a crash can't leave a half-written file."""
        with self._lock:
            directory = os.path.dirname(os.path.abspath(self.path))
            fd, tmp = tempfile.mkstemp(prefix=".keyswap-", dir=directory)
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as fh:
                    json.dump(state, fh, indent=2)
                os.chmod(tmp, 0o600)
                os.replace(tmp, self.path)
            except OSError:
                log.exception("could not save key state to %s", self.path)
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
