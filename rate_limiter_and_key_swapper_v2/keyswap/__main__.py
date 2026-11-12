"""`python -m keyswap` starts the HTTP proxy, configured from environment variables / a .env file."""

import logging
import os

from .proxy import app_from_env, load_env


def main() -> None:
    import uvicorn

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    env = load_env()
    app = app_from_env()
    # One worker on purpose: key health is per-process state.
    uvicorn.run(app, host=env.get("HOST", "127.0.0.1"), port=int(env.get("PORT", "8000")), workers=1)


if __name__ == "__main__":
    main()
