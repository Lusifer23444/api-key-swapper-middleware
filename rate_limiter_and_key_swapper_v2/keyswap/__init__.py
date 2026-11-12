"""keyswap v2: health-aware API key rotation with separate 429 / 503 handling and model fallbacks."""

from .errors import (
    AllKeysUnavailable,
    DeadlineExceeded,
    GatewayBusy,
    KeySwapError,
    ModelUnavailable,
    ProviderUnavailable,
    ResponseTooLarge,
)
from .gateway import Gateway, GatewayConfig, GatewayResponse, GatewayStream, Provider
from .models import ModelMap
from .pool import KeyPool, KeyState, Outcome, PoolConfig
from .sources import keys_from_env, keys_from_file, parse_dotenv, read_dotenv
from .store import JsonStateStore

__version__ = "2.0.0"

__all__ = [
    "AllKeysUnavailable", "DeadlineExceeded", "Gateway", "GatewayBusy", "GatewayConfig",
    "GatewayResponse", "GatewayStream", "JsonStateStore", "KeyPool", "KeyState", "KeySwapError",
    "ModelMap", "ModelUnavailable", "Outcome", "PoolConfig", "Provider", "ProviderUnavailable",
    "ResponseTooLarge", "keys_from_env", "keys_from_file", "parse_dotenv", "read_dotenv",
]
