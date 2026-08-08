import json
import logging
import os
import signal
import sys
import time

import psycopg2
from confluent_kafka import Consumer, KafkaException, TopicPartition

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
POSTGRES_DSN = os.environ.get(
    "POSTGRES_DSN",
    "host=postgres dbname=streamingdb user=postgres password=postgres",
)

_stop_requested = False
_db_connection = None


def handle_signal(signum, frame):
    global _stop_requested
    logger.info("Received signal %d; shutting down", signum)
    _stop_requested = True


def get_connection():
    global _db_connection
    if _db_connection is not None and not _db_connection.closed:
        return _db_connection
    if _db_connection is not None:
        logger.info("Disconnected from Postgres; reconnecting")
        try:
            _db_connection.close()
        except Exception:
            pass
        _db_connection = None
    else:
        logger.info("Connecting to Postgres")
    try:
        _db_connection = psycopg2.connect(POSTGRES_DSN)
        _db_connection.autocommit = True
    except Exception:
        _db_connection = None
        raise
    logger.info("Connected to Postgres")
    return _db_connection


def flush_batch(events, consumer):
    global _db_connection
    if not events:
        return
    placeholders = ",".join(["(%s, %s, %s, %s, %s)"] * len(events))
    sql = (
        "INSERT INTO transaction_events "
        "(event_id, event_timestamp, account_id, amount, transaction_type) "
        "VALUES " + placeholders + " ON CONFLICT DO NOTHING"
    )
    params = []
    for _, event in events:
        params.extend(
            [
                event["event_id"],
                event["event_timestamp"],
                event["account_id"],
                event["amount"],
                event["transaction_type"],
            ]
        )
    try:
        conn = get_connection()
        started = time.monotonic()
        with conn.cursor() as cursor:
            cursor.execute(sql, params)
        duration_ms = (time.monotonic() - started) * 1000
    except psycopg2.Error as exc:
        logger.error("Batch INSERT failed: %s", exc)
        if _db_connection is not None:
            try:
                _db_connection.close()
            except Exception:
                pass
            _db_connection = None
        raise
    offsets = [msg for msg, _ in events]
    try:
        consumer.commit(
            offsets=[
                TopicPartition(m.topic(), m.partition(), m.offset() + 1) for m in offsets
            ],
            asynchronous=False,
        )
    except KafkaException as exc:
        logger.error("Offset commit failed: %s", exc)
        raise
    logger.info(
        "Inserted %d events in %.2f ms; committed offsets up to %d",
        len(events),
        duration_ms,
        max(msg.offset() for msg in offsets),
    )


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
    try:
        get_connection()
    except Exception as exc:
        logger.error("Could not connect to Postgres at startup: %s", exc)
    buffer = []
    last_flush = time.monotonic()

    def do_flush():
        nonlocal last_flush
        now = time.monotonic()
        logger.info(
            "Flushing %d events, %.3fs since last flush", len(buffer), now - last_flush
        )
        try:
            get_connection()
        except Exception as exc:
            logger.error("Postgres unavailable; deferring flush: %s", exc)
            return
        try:
            flush_batch(buffer, consumer)
        except Exception:
            logger.error("Batch flush failed; retaining buffer for retry")
            return
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
            buffer.append((msg, event))
            if len(buffer) >= BATCH_SIZE:
                do_flush()
    except KeyboardInterrupt:
        logger.info("Interrupted")
    finally:
        consumer.close()
        if _db_connection is not None:
            _db_connection.close()
        logger.info("Consumer stopped cleanly")


if __name__ == "__main__":
    main()
