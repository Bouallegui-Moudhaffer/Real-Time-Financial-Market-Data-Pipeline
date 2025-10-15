from __future__ import annotations

import os
import time
from typing import Optional, Dict, Any

import orjson
from confluent_kafka import Producer
from jsonschema import validate, ValidationError

from config import Settings
from util import logger, json_dumps


class KafkaTradesProducer:
    def __init__(self, settings: Settings) -> None:
        conf = {
            "bootstrap.servers": settings.kafka_brokers,
            "client.id": settings.kafka_client_id,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": int(os.environ.get("KAFKA_LINGER_MS", "25")),
            "batch.num.messages": int(os.environ.get("KAFKA_BATCH_NUM_MESSAGES", "1000")),
            "compression.type": os.environ.get("KAFKA_COMPRESSION", "zstd"),
            "message.timeout.ms": int(os.environ.get("KAFKA_MESSAGE_TIMEOUT_MS", "60000")),
            "delivery.timeout.ms": int(os.environ.get("KAFKA_DELIVERY_TIMEOUT_MS", "120000")),
        }
        if settings.kafka_security_protocol and settings.kafka_security_protocol != "PLAINTEXT":
            conf.update(
                {
                    "security.protocol": settings.kafka_security_protocol,
                }
            )
            if settings.kafka_sasl_mechanism:
                conf["sasl.mechanism"] = settings.kafka_sasl_mechanism
            if settings.kafka_sasl_username:
                conf["sasl.username"] = settings.kafka_sasl_username
            if settings.kafka_sasl_password:
                conf["sasl.password"] = settings.kafka_sasl_password

        self._topic = settings.topic_trades_raw
        self._producer = Producer(conf)
        self._sent = 0
        self._errors = 0

    def _dr_cb(self, err, msg) -> None:
        if err is not None:
            self._errors += 1
            logger.error(
                {
                    "msg": "delivery_failed",
                    "error": str(err),
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                }
            )
        else:
            self._sent += 1
            # Keep logs light; uncomment for verbose delivery logs
            # logger.info({"msg":"delivery_ok","topic": msg.topic(), "offset": msg.offset()})

    def validate_and_send(self, trade: Dict[str, Any], schema: Dict[str, Any]) -> None:
        try:
            validate(instance=trade, schema=schema)
        except ValidationError as e:
            self._errors += 1
            logger.error({"msg": "schema_validation_failed", "error": str(e), "trade": trade})
            return

        key = trade["symbol"].encode("utf-8")
        payload = orjson.dumps(trade)
        try:
            self._producer.produce(
                topic=self._topic,
                key=key,
                value=payload,
                on_delivery=self._dr_cb,
            )
        except BufferError:
            # Backpressure
            self._producer.poll(0.1)
            self._producer.produce(
                topic=self._topic, key=key, value=payload, on_delivery=self._dr_cb
            )
        self._producer.poll(0)  # serve delivery callbacks

    def flush(self) -> None:
        self._producer.flush(10)
        logger.info({"msg": "producer_flush", "sent": self._sent, "errors": self._errors})

    def close(self) -> None:
        self.flush()
