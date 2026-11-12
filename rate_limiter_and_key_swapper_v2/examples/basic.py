"""Minimal usage. Your app supplies its own keys from wherever it keeps secrets (no .env needed)."""

import asyncio
import os

from keyswap import AllKeysUnavailable, Gateway, JsonStateStore, ModelMap, Provider, ProviderUnavailable


async def main() -> None:
    models = ModelMap({
        "fast": ["gpt-4o-mini", "gpt-4.1-mini"],   # first name that still exists wins
        "smart": ["gpt-4o", "gpt-4.1"],
    })
    async with Gateway(Provider.openai(), models=models, store=JsonStateStore("key_health.json")) as gw:
        for secret in os.environ["MY_KEYS"].split(","):    # however YOUR app loads secrets
            gw.add_key(secret)

        try:
            reply = await gw.chat({"model": "fast", "messages": [{"role": "user", "content": "Say hi"}]})
            print(reply.json()["choices"][0]["message"]["content"], "| key:", reply.key_id)
        except AllKeysUnavailable as exc:
            print("All keys busy, retry in", exc.retry_after, "seconds")
        except ProviderUnavailable as exc:
            print("Provider is down (keys not blamed):", exc)

        print(gw.status())   # ids and states only, never secrets


asyncio.run(main())
