from __future__ import annotations

import asyncio
import os
import signal
import sys
import time
from typing import Iterable, List

from util import (
    load_trade_schema,
    normalize_finnhub_trade_msg,
    build_trade_record,
    json_dumps,
    logger,
)
from kafkaio import KafkaTradesProducer
from wsclient import FinnhubWsClient
from config import Settings


SHUTDOWN = asyncio.Event()


def _install_signals() -> None:
    def handler(signum, frame):
        logger.info({"msg": "signal_received", "signal": signum})
        SHUTDOWN.set()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


async def run_replay(settings: Settings, producer: KafkaTradesProducer) -> None:
    """
    Stream canned JSON lines into Kafka. Each line is a Finnhub-style envelope
    {"type":"trade","data":[{...}]}
    """
    path = os.environ.get(
        "REPLAY_FILE", os.path.join(os.path.dirname(__file__), "data", "replay_trades.jsonl")
    )
    schema = load_trade_schema()
    logger.info({"msg": "replay_start", "file": path, "topic": settings.topic_trades_raw})
    count = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            records = normalize_finnhub_trade_msg(line)
            for rec in records:
                # Convert to our schema'd trade record (adds ingest_ts, key)
                trade = build_trade_record(
                    symbol=rec["symbol"],
                    price=rec["price"],
                    volume=rec["volume"],
                    event_ts_ms=rec["event_ts"],
                    source="replay",
                )
                producer.validate_and_send(trade, schema)
                count += 1
            await asyncio.sleep(0)  # yield to loop
    producer.flush()
    logger.info({"msg": "replay_complete", "produced": count})


async def run_live(settings: Settings, producer: KafkaTradesProducer) -> None:
    """
    Connect to Finnhub WS, subscribe to symbols, and publish trades to Kafka.
    Robust reconnect with exponential backoff + jitter.
    """
    schema = load_trade_schema()
    symbols = [s.strip().upper() for s in settings.symbols.split(",") if s.strip()]
    ws = FinnhubWsClient(
        token=settings.finnhub_api_token,
        symbols=symbols,
        ping_interval=float(os.environ.get("WS_PING_SECONDS", "15")),
        connect_timeout=float(os.environ.get("WS_CONNECT_TIMEOUT", "15")),
        recv_timeout=float(os.environ.get("WS_RECV_TIMEOUT", "30")),
    )

    async for ev in ws.run_forever(stop_event=SHUTDOWN):
        # ev is a raw JSON string from Finnhub
        try:
            records = normalize_finnhub_trade_msg(ev)
            for rec in records:
                trade = build_trade_record(
                    symbol=rec["symbol"],
                    price=rec["price"],
                    volume=rec["volume"],
                    event_ts_ms=rec["event_ts"],
                    source="finnhub",
                )
                producer.validate_and_send(trade, schema)
        except Exception as e:
            logger.error({"msg": "normalize_or_produce_error", "error": str(e), "raw": ev})
        if SHUTDOWN.is_set():
            break

    producer.flush()
    logger.info({"msg": "live_complete"})


async def main_async() -> int:
    settings = Settings()
    _install_signals()
    mode = os.environ.get("PRODUCER_MODE", "live").lower()
    # Kafka Producer
    producer = KafkaTradesProducer(settings)
    logger.info(
        {
            "msg": "producer_start",
            "mode": mode,
            "topic": settings.topic_trades_raw,
            "symbols": settings.symbols,
            "brokers": settings.kafka_brokers,
        }
    )
    try:
        if mode == "replay":
            await run_replay(settings, producer)
        elif mode == "live":
            if not settings.finnhub_api_token:
                logger.error({"msg": "missing_finnhub_token"})
                return 1
            await run_live(settings, producer)
        else:
            logger.error({"msg": "unknown_mode", "mode": mode})
            return 2
        return 0
    finally:
        producer.close()


def main() -> None:
    try:
        rc = asyncio.run(main_async())
    except KeyboardInterrupt:
        rc = 130
    sys.exit(rc)


if __name__ == "__main__":
    main()
