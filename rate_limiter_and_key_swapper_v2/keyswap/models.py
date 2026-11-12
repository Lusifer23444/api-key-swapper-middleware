"""Model aliases with fallback chains: the fix for "the provider renamed my model and my app died".

Your code asks for an alias ("fast", "smart"), never a raw model name. The alias maps to an
ordered list of real model names. When the provider says a model no longer exists, that model
is skipped for a while and the next one in the chain is used, with a loud warning in the log
so a human updates the map.

Honest limit: this contains a rename or retirement, it can't predict an API format change.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Callable, Iterable, Optional

log = logging.getLogger("keyswap")


class ModelMap:
    def __init__(
        self,
        aliases: Optional[dict[str, list[str]]] = None,
        *,
        unavailable_ttl_s: float = 3600.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._aliases: dict[str, list[str]] = {}
        self._dead_until: dict[str, float] = {}
        self._ttl = unavailable_ttl_s
        self._clock = clock
        self._lock = threading.Lock()
        for alias, chain in (aliases or {}).items():
            self.set_alias(alias, chain)

    def set_alias(self, alias: str, models: Iterable[str]) -> None:
        """Create or replace an alias at runtime. Models are tried in the order given."""
        chain = [m for m in models if m]
        if not alias or not chain:
            raise ValueError("An alias needs a name and at least one model.")
        with self._lock:
            self._aliases[alias] = chain
            for m in chain:          # a fresh chain is a human's fix: forget old "unavailable" marks
                self._dead_until.pop(m, None)

    def is_alias(self, name: str) -> bool:
        with self._lock:
            return name in self._aliases

    def resolve(self, name: str) -> Optional[str]:
        """Alias -> first model not known to be gone (None if all are). Non-alias -> returned unchanged."""
        with self._lock:
            chain = self._aliases.get(name)
            if chain is None:
                return name
            now = self._clock()
            for model in chain:
                if self._dead_until.get(model, 0.0) <= now:
                    return model
            return None

    def mark_unavailable(self, model: str) -> None:
        with self._lock:
            self._dead_until[model] = self._clock() + self._ttl
        log.warning(
            "MODEL %r was rejected by the provider (renamed or retired?). Falling back along the alias "
            "chain; update your ModelMap.", model,
        )

    def aliases(self) -> dict[str, list[str]]:
        with self._lock:
            return {a: list(c) for a, c in self._aliases.items()}
