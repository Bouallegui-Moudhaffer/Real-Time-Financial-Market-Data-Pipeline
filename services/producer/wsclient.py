from __future__ import annotations

import asyncio
import json
import os
import random
from typing import AsyncGenerator, List

import websockets
from websockets.exceptions import ConnectionClosed

from util import logger


class FinnhubWsClient:
    def __init__(
        self,
        token: str,
        symbols: List[str],
        ping_interval: float = 15.0,
        connect_timeout: float = 15.0,
        recv_timeout: float = 30.0,
    ) -> None:
        self.token = token
        self.symbols = symbols
        self.ping_interval = ping_interval
        self.connect_timeout = connect_timeout
        self.recv_timeout = recv_timeout

    async def _subscribe(self, ws: websockets.WebSocketClientProtocol) -> None:
        for s in self.symbols:
            await ws.send(json.dumps({"type": "subscribe", "symbol": s}))
            await asyncio.sleep(0.05)  # tiny spacing to be polite

    async def _ping_task(self, ws: websockets.WebSocketClientProtocol, stop: asyncio.Event):
        try:
            while not stop.is_set():
                await asyncio.sleep(self.ping_interval)
                try:
                    pong_waiter = await ws.ping()
                    await asyncio.wait_for(pong_waiter, timeout=10)
                except Exception as e:
                    logger.warning({"msg": "ping_failed", "error": str(e)})
                    return
        except asyncio.CancelledError:
            return

    async def run_forever(self, stop_event: asyncio.Event) -> AsyncGenerator[str, None]:
        """
        Yields raw JSON messages from Finnhub WS until stop_event is set.
        Handles reconnection with exponential backoff + jitter.
        """
        backoff = 1.0
        while not stop_event.is_set():
            url = f"wss://ws.finnhub.io?token={self.token}"
            logger.info({"msg": "ws_connecting", "url": "wss://ws.finnhub.io", "symbols": self.symbols})
            try:
                async with websockets.connect(url, open_timeout=self.connect_timeout) as ws:
                    logger.info({"msg": "ws_connected"})
                    await self._subscribe(ws)
                    stop_local = asyncio.Event()
                    ping_task = asyncio.create_task(self._ping_task(ws, stop_local))
                    backoff = 1.0  # reset backoff after successful connect

                    while not stop_event.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=self.recv_timeout)
                        except asyncio.TimeoutError:
                            # If no message, send a ping check by awaiting ping_task cycle
                            logger.debug({"msg": "ws_idle_timeout"})
                            continue
                        if not raw:
                            continue
                        yield raw
            except (ConnectionClosed, OSError, asyncio.TimeoutError) as e:
                logger.warning({"msg": "ws_disconnected", "error": str(e)})
                # fall through to backoff/retry
            except Exception as e:
                logger.error({"msg": "ws_unexpected_error", "error": str(e)})
            # Backoff with jitter
            delay = min(60.0, backoff + random.uniform(0, 0.5 * backoff))
            logger.info({"msg": "ws_reconnect_backoff", "seconds": round(delay, 2)})
            await asyncio.sleep(delay)
            backoff = min(60.0, backoff * 2.0)
