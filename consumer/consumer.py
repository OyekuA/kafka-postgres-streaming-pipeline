import json
import logging
import os
import signal
import sys
import time

from confluent_kafka import Consumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("consumer")

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "raw-logs")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "log-consumer-group")
CONSUMER_POLL_TIMEOUT = float(os.environ.get("CONSUMER_POLL_TIMEOUT", "1.0"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))
FLUSH_INTERVAL_SECONDS = int(os.environ.get("FLUSH_INTERVAL_SECONDS", "5"))

_stop_requested = False


def handle_signal(signum, frame):
    global _stop_requested
    logger.info("Received signal %d; shutting down", signum)
    _stop_requested = True


def flush_batch(events):
    logger.info("Flushing batch of %d events", len(events))


def main():
    signal.signal(signal.SIGTERM, handle_signal)
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": KAFKA_GROUP_ID,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([KAFKA_TOPIC])
    logger.info(
        "Consuming from topic '%s' as group '%s' at %s",
        KAFKA_TOPIC,
        KAFKA_GROUP_ID,
        BOOTSTRAP_SERVERS,
    )
    buffer = []
    last_flush = time.monotonic()

    def do_flush():
        nonlocal last_flush
        now = time.monotonic()
        logger.info(
            "Flushing %d events, %.3fs since last flush", len(buffer), now - last_flush
        )
        flush_batch(buffer)
        buffer.clear()
        last_flush = now

    try:
        while not _stop_requested:
            if buffer and time.monotonic() - last_flush >= FLUSH_INTERVAL_SECONDS:
                do_flush()
            msg = consumer.poll(timeout=CONSUMER_POLL_TIMEOUT)
            if msg is None:
                continue
            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue
            if msg.value() is None:
                continue
            value = msg.value().decode("utf-8")
            logger.debug("Received message: %s", value)
            try:
                event = json.loads(value)
            except json.JSONDecodeError:
                logger.warning("Skipping message with invalid JSON")
                continue
            buffer.append(event)
            if len(buffer) >= BATCH_SIZE:
                do_flush()
    except KeyboardInterrupt:
        logger.info("Interrupted")
    finally:
        consumer.close()
        logger.info("Consumer stopped cleanly")


if __name__ == "__main__":
    main()
