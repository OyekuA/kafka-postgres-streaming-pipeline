import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Producer
from faker import Faker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("producer")

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("TOPIC", "raw-logs")
EVENTS_PER_SECOND = int(os.environ.get("EVENTS_PER_SECOND", "10000"))
DURATION_SECONDS = int(os.environ.get("DURATION_SECONDS", "0"))
WINDOW_SECONDS = 1.0
TRANSACTION_TYPES = ("deposit", "withdrawal", "transfer", "payment")

fake = Faker()

_stop_requested = False
_delivery_counts = {"delivered": 0, "failed": 0}


def delivery_callback(err, msg):
    if err is not None:
        _delivery_counts["failed"] += 1
        logger.error("Message delivery failed: %s", err)
    else:
        _delivery_counts["delivered"] += 1
        logger.debug(
            "Message delivered to %s [partition %s] at offset %s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


def build_event():
    return {
        "event_id": fake.uuid4(),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "account_id": fake.uuid4().replace("-", ""),
        "amount": round(fake.random.uniform(0.01, 10000.0), 2),
        "transaction_type": fake.random_element(TRANSACTION_TYPES),
    }


def handle_signal(signum, frame):
    global _stop_requested
    logger.info("Received signal %d; shutting down", signum)
    _stop_requested = True


def shutdown(producer, reason):
    logger.info("%s; flushing pending messages", reason)
    pending = producer.flush(timeout=10.0)
    if pending:
        logger.warning("%d message(s) still pending after flush", pending)
    producer.close()
    logger.info("Producer stopped cleanly")


def main():
    signal.signal(signal.SIGTERM, handle_signal)
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "acks": "all"})
    logger.info(
        "Connecting to Kafka at %s, producing %d events/sec to topic '%s' "
        "(duration: %s)",
        BOOTSTRAP_SERVERS,
        EVENTS_PER_SECOND,
        TOPIC,
        "indefinite" if DURATION_SECONDS == 0 else f"{DURATION_SECONDS}s",
    )
    started = time.monotonic()
    try:
        while not _stop_requested:
            batch_start = time.perf_counter()
            events = [build_event() for _ in range(EVENTS_PER_SECOND)]
            for event in events:
                producer.produce(
                    TOPIC,
                    value=json.dumps(event).encode("utf-8"),
                    callback=delivery_callback,
                )
                producer.poll(0)
            elapsed = time.perf_counter() - batch_start
            delivered = _delivery_counts["delivered"]
            failed = _delivery_counts["failed"]
            _delivery_counts["delivered"] = 0
            _delivery_counts["failed"] = 0
            logger.info(
                "Sent %d events in %.3fs (~%.0f events/sec, %d delivered, %d failed)",
                len(events),
                elapsed,
                len(events) / elapsed,
                delivered,
                failed,
            )
            if DURATION_SECONDS > 0 and time.monotonic() - started >= DURATION_SECONDS:
                break
            sleep_leftover = WINDOW_SECONDS - elapsed
            if sleep_leftover > 0:
                time.sleep(sleep_leftover)
    except KeyboardInterrupt:
        shutdown(producer, "Interrupted")
    else:
        shutdown(producer, "Shutdown requested" if _stop_requested else "Duration elapsed")


if __name__ == "__main__":
    main()
