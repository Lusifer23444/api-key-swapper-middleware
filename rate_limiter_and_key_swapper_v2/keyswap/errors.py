"""Errors raised by keyswap. Everything inherits from KeySwapError so callers can catch one type."""

from __future__ import annotations

from typing import Optional


class KeySwapError(Exception):
    """Base class for every error keyswap raises on its own."""


class AllKeysUnavailable(KeySwapError):
    """No key can serve the request right now.

    retry_after: seconds until the soonest key may be usable again, or None if no key can
    ever recover on its own (empty pool, or every key disabled).
    """

    def __init__(self, retry_after: Optional[float], keys: list[dict]) -> None:
        self.retry_after = retry_after
        self.keys = keys
        if retry_after is None:
            msg = "No usable keys: the pool is empty or every key is disabled (check key states)."
        else:
            msg = f"All keys are cooling down. Soonest one is available in about {retry_after:.0f}s."
        super().__init__(msg)


class ModelUnavailable(KeySwapError):
    """Every model in an alias's fallback chain was reported missing by the provider."""

    def __init__(self, alias: str) -> None:
        self.alias = alias
        super().__init__(
            f"No available model for alias {alias!r}: the provider rejected every model in its "
            f"fallback chain. Update the alias with ModelMap.set_alias()."
        )


class ProviderUnavailable(KeySwapError):
    """The provider kept failing (503/502/504/529 or network errors) after the allowed retries.

    This is the provider's problem, not a key's, so no key was penalised.
    """

    def __init__(self, last_status: Optional[int]) -> None:
        self.last_status = last_status
        what = f"HTTP {last_status}" if last_status is not None else "a network error"
        super().__init__(f"Provider unavailable after retries (last failure: {what}). Keys were not penalised.")


class DeadlineExceeded(KeySwapError):
    """The overall request deadline ran out before any attempt succeeded."""


class GatewayBusy(KeySwapError):
    """Every connection to the provider is in use (local saturation, not the provider's fault)."""

    def __init__(self) -> None:
        super().__init__("Connection pool is saturated; retry in a moment or raise max_connections.")


class ResponseTooLarge(KeySwapError):
    """The provider's response was bigger than GatewayConfig.max_response_bytes."""
