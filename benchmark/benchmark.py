import json
import logging
import os
import re
import subprocess
import sys
import time
from pathlib import Path

import psycopg2
from confluent_kafka import Producer

ROOT = Path(__file__).resolve().parent.parent
COMPOSE_FILE = ROOT / "docker-compose.yml"

sys.path.insert(0, str(ROOT))
from producer.producer import build_event

EVENT_COUNT = 10000
SETTLE_SECONDS = 2.0
BATCH_LINE_RE = re.compile(r"Inserted (\d+) events in")

TOPIC = os.environ.get("TOPIC", "raw-logs")
GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "log-consumer-group")
BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9093")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))
POLL_INTERVAL_SECONDS = float(os.environ.get("POLL_INTERVAL_SECONDS", "1.0"))
TIMEOUT_SECONDS = int(os.environ.get("TIMEOUT_SECONDS", "300"))
POSTGRES_DSN = os.environ.get(
    "POSTGRES_DSN",
    "host=localhost port=5432 dbname=streamingdb user=postgres password=postgres",
)

logger = logging.getLogger("benchmark")


class DockerCompose:
    def __init__(self, compose_file):
        self.compose_file = compose_file

    def run(self, *args, env=None, check=True):
        full_env = os.environ.copy()
        if env:
            full_env.update(env)
        result = subprocess.run(
            ["docker", "compose", "-f", str(self.compose_file), *args],
            env=full_env,
            capture_output=True,
            text=True,
        )
        if check and result.returncode != 0:
            raise RuntimeError(
                f"docker compose {' '.join(args)} failed ({result.returncode}): "
                f"{result.stderr.strip()}"
            )
        return result


def configure_logging():
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.handlers[:] = [handler]
    logger.setLevel(logging.INFO)
    logger.propagate = False


def check_services_running(compose):
    result = compose.run(
        "ps", "--status", "running", "-q", "kafka", "postgres", "consumer", "producer"
    )
    running = [line for line in result.stdout.splitlines() if line.strip()]
    if len(running) < 4:
        raise RuntimeError(
            "Not all required services are running; start the stack with "
            "'docker compose up -d' first"
        )


def stop_services(compose):
    compose.run("stop", "producer", "consumer")


def reset_consumer_offsets(compose, topic, group_id):
    args = (
        "exec",
        "-T",
        "kafka",
        "kafka-consumer-groups",
        "--bootstrap-server",
        "kafka:9092",
        "--group",
        group_id,
        "--reset-offsets",
        "--to-latest",
        "--topic",
        topic,
        "--execute",
    )
    for attempt in range(3):
        result = compose.run(*args, check=False)
        if result.returncode == 0:
            return
        logger.warning(
            "Offset reset attempt %d failed: %s",
            attempt + 1,
            result.stderr.strip(),
        )
        time.sleep(5)
    raise RuntimeError(
        f"Failed to reset offsets for consumer group '{group_id}'; "
        "the group must exist with no active members, so the consumer "
        "service must have run at least once before benchmarking"
    )


def start_consumer(compose, batch_size):
    compose.run(
        "up", "-d", "--force-recreate", "consumer",
        env={"BATCH_SIZE": str(batch_size)},
    )


def restore_services(compose, batch_size):
    for service, env in (
        ("consumer", {"BATCH_SIZE": str(batch_size)}),
        ("producer", None),
    ):
        try:
            compose.run("up", "-d", service, env=env)
        except Exception as exc:
            logger.warning("Failed to restore service %s: %s", service, exc)


def format_timeout_label(timeout_seconds):
    if timeout_seconds % 60 == 0:
        return f"{timeout_seconds // 60} min"
    return f"{timeout_seconds} s"


def connect_db(dsn):
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    return conn


def truncate_table(conn):
    with conn.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE transaction_events")


def poll_row_count(conn):
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM transaction_events")
        return cursor.fetchone()[0]


def produce_events(bootstrap_servers, topic, count):
    producer = Producer({"bootstrap.servers": bootstrap_servers, "acks": "all"})
    failures = {"count": 0}

    def on_delivery(err, msg):
        if err is not None:
            failures["count"] += 1
            logger.error("Message delivery failed: %s", err)

    for _ in range(count):
        producer.produce(
            topic,
            value=json.dumps(build_event()).encode("utf-8"),
            callback=on_delivery,
        )
        producer.poll(0)
    pending = producer.flush(timeout=120)
    producer.close()
    if pending > 0 or failures["count"] > 0:
        raise RuntimeError(
            f"{pending} message(s) still pending and {failures['count']} "
            "delivery failure(s) while producing events"
        )


def wait_for_rows(conn, target, started, timeout_seconds, poll_interval):
    deadline = started + timeout_seconds
    last_rows = 0
    while True:
        try:
            last_rows = poll_row_count(conn)
        except Exception as exc:
            logger.warning("Poll failed, retrying: %s", exc)
        now = time.monotonic()
        elapsed = now - started
        logger.info("Polled %d rows after %.1f seconds", last_rows, elapsed)
        if last_rows >= target:
            return last_rows, elapsed
        if now >= deadline:
            return last_rows, elapsed
        time.sleep(poll_interval)


def count_batches_from_logs(compose):
    result = compose.run("logs", "consumer")
    batch_count = 0
    inserted_total = 0
    for line in result.stdout.splitlines():
        match = BATCH_LINE_RE.search(line)
        if match:
            batch_count += 1
            inserted_total += int(match.group(1))
    return batch_count, inserted_total


def main():
    configure_logging()
    try:
        run_trial()
    except Exception as exc:
        logger.exception("Benchmark failed: %s", exc)
        raise SystemExit(2)


def run_trial():
    compose = DockerCompose(COMPOSE_FILE)
    check_services_running(compose)
    conn = connect_db(POSTGRES_DSN)
    batch_count = None
    inserted_total = 0
    try:
        stop_services(compose)
        truncate_table(conn)
        reset_consumer_offsets(compose, TOPIC, GROUP_ID)
        start_consumer(compose, BATCH_SIZE)
        started = time.monotonic()
        produce_events(BOOTSTRAP_SERVERS, TOPIC, EVENT_COUNT)
        rows, elapsed = wait_for_rows(
            conn, EVENT_COUNT, started, TIMEOUT_SECONDS, POLL_INTERVAL_SECONDS
        )
        if rows >= EVENT_COUNT:
            time.sleep(SETTLE_SECONDS)
            batch_count, inserted_total = count_batches_from_logs(compose)
    finally:
        restore_services(compose, BATCH_SIZE)
        conn.close()
    if batch_count is not None:
        events_per_second = EVENT_COUNT / elapsed
        logger.info(
            "Benchmark complete: %d events in %.2f s (%.1f events/sec, %d batches)",
            EVENT_COUNT,
            elapsed,
            events_per_second,
            batch_count,
        )
        if inserted_total != EVENT_COUNT:
            logger.warning(
                "Consumer logs show %d events inserted across %d batches",
                inserted_total,
                batch_count,
            )
        if batch_count == 0:
            logger.warning("Could not determine batch count from consumer logs")
        summary = {
            "status": "success",
            "events": EVENT_COUNT,
            "batch_size": BATCH_SIZE,
            "elapsed_seconds": round(elapsed, 3),
            "events_per_second": round(events_per_second, 2),
            "batch_count": batch_count,
            "rows_committed": rows,
        }
        print(json.dumps(summary))
        return
    logger.error(
        f"Only {rows:,}/{EVENT_COUNT:,} rows committed after "
        f"{format_timeout_label(TIMEOUT_SECONDS)}"
    )
    summary = {
        "status": "timeout",
        "events": EVENT_COUNT,
        "batch_size": BATCH_SIZE,
        "elapsed_seconds": round(elapsed, 3),
        "events_per_second": None,
        "batch_count": None,
        "rows_committed": rows,
    }
    print(json.dumps(summary))
    raise SystemExit(1)


if __name__ == "__main__":
    main()
