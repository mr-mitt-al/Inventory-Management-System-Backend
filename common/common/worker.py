"""Runner for the non-HTTP processes: consumers, the outbox publisher, sweepers.

Handles the boring-but-load-bearing part - SIGTERM from ``docker compose down``
must let an in-flight handler finish and commit its offset, otherwise every
restart redelivers work and leaks reservations.
"""

from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import Awaitable, Callable

logger = logging.getLogger(__name__)

AsyncMain = Callable[[asyncio.Event], Awaitable[None]]


def run_worker(main: AsyncMain, *, name: str) -> None:
    """Run ``main`` until SIGINT/SIGTERM, then let it shut down gracefully.

    ``main`` receives a stop event and is expected to observe it.
    """
    asyncio.run(_run(main, name=name))


async def _run(main: AsyncMain, *, name: str) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_stop(signame: str) -> None:
        logger.info("shutdown signal received", extra={"worker": name, "signal": signame})
        stop.set()

    for signame in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, signame, None)
        if sig is None:
            continue
        try:
            loop.add_signal_handler(sig, _request_stop, signame)
        except NotImplementedError:
            # Windows: add_signal_handler is unsupported on ProactorEventLoop.
            # Containers are Linux, so this only affects running a worker
            # directly on a Windows host - Ctrl+C still works, just less gently.
            signal.signal(sig, lambda *_: _request_stop(signame))

    logger.info("worker starting", extra={"worker": name})
    try:
        await main(stop)
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("worker stopped", extra={"worker": name})
