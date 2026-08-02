import json
import logging
import os
import sys
from datetime import datetime, timezone
from uuid import uuid4

from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("producer")

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("TOPIC", "raw-logs")


def delivery_callback(err, msg):
    if err is not None:
        logger.error("Message delivery failed: %s", err)
    else:
        logger.info(
            "Message delivered to %s [partition %s] at offset %s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


def build_event():
    return {
        "event_id": str(uuid4()),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "account_id": "acct-001",
        "amount": 1234.56,
        "transaction_type": "payment",
    }


def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    event = build_event()
    logger.info(
        "Connecting to Kafka at %s, sending 1 event to topic '%s'",
        BOOTSTRAP_SERVERS,
        TOPIC,
    )
    producer.produce(TOPIC, value=json.dumps(event).encode("utf-8"), callback=delivery_callback)
    pending = producer.flush(timeout=10.0)
    if pending:
        logger.error("Flush timed out with %d message(s) pending delivery", pending)
        sys.exit(1)
    logger.info("Sent event: %s", json.dumps(event))
    logger.info("Producer flushed successfully; exiting cleanly")


if __name__ == "__main__":
    main()
