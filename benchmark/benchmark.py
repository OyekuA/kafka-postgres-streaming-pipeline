import json
import logging
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from confluent_kafka import Producer

ROOT = Path(__file__).resolve().parent.parent
COMPOSE_FILE = ROOT / "docker-compose.yml"
RESULTS_DIR = ROOT / "benchmark" / "results"

sys.path.insert(0, str(ROOT))
from producer.producer import build_event

EVENT_COUNT = 10000
SETTLE_SECONDS = 2.0
INDIVIDUAL_BATCH_SIZE = 1
BATCH_LINE_RE = re.compile(r"Inserted (\d+) events in ([\d.]+) ms")

TOPIC = os.environ.get("TOPIC", "raw-logs")
GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "log-consumer-group")
BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9093")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))
SINGLE_TRIAL = os.environ.get("SINGLE_TRIAL", "0") == "1"
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
    durations_ms = []
    for line in result.stdout.splitlines():
        match = BATCH_LINE_RE.search(line)
        if match:
            batch_count += 1
            inserted_total += int(match.group(1))
            durations_ms.append(float(match.group(2)))
    return batch_count, inserted_total, durations_ms


def main():
    configure_logging()
    try:
        results = run_benchmark()
    except Exception as exc:
        logger.exception("Benchmark failed: %s", exc)
        raise SystemExit(2)
    write_results(results)
    if len(results) == 2 and all(r["status"] == "success" for r in results):
        generate_chart(results)
    if len(results) == 2:
        print_comparison(results)
    if results[-1]["status"] == "timeout":
        raise SystemExit(1)


def run_benchmark():
    batch_sizes = (
        [BATCH_SIZE, INDIVIDUAL_BATCH_SIZE] if not SINGLE_TRIAL else [BATCH_SIZE]
    )
    results = []
    for batch_size in batch_sizes:
        results.append(run_trial(batch_size))
        if results[-1]["status"] == "timeout":
            break
    return results


def fmt_value(value):
    if value is None:
        return "n/a"
    if isinstance(value, str):
        return value
    if isinstance(value, int):
        return f"{value:,}"
    return f"{value:,.2f}"


def trial_label(index, batch_size):
    mode = "Batch" if index == 0 else "Individual"
    return f"{mode} ({batch_size})"


def print_comparison(results):
    headers = [
        "Metric",
        trial_label(0, results[0]["batch_size"]),
        trial_label(1, results[1]["batch_size"]),
    ]
    rows = [
        ("Total time (s)", "elapsed_seconds"),
        ("Throughput (events/s)", "events_per_second"),
        ("Batch count", "batch_count"),
        ("Avg INSERT time (ms)", "avg_batch_insert_ms"),
        ("Rows committed", "rows_committed"),
        ("Status", "status"),
    ]
    lines = [headers]
    for label, key in rows:
        cells = [label]
        for r in results:
            if key == "elapsed_seconds" and r["status"] != "success":
                cells.append("n/a")
            else:
                cells.append(fmt_value(r[key]))
        lines.append(cells)
    widths = [max(len(cells[i]) for cells in lines) for i in range(len(headers))]
    print()
    for cells in lines:
        print("  ".join(cell.ljust(widths[i]) for i, cell in enumerate(cells)))
    if not all(r["status"] == "success" for r in results):
        return
    ratio = results[1]["elapsed_seconds"] / results[0]["elapsed_seconds"]
    if ratio > 1:
        verdict = "batch mode is faster"
    elif ratio < 1:
        verdict = "individual mode is faster"
    else:
        verdict = "no difference"
    print(
        f"\nSpeedup ratio (individual time / batch time): {ratio:.2f}x "
        f"({verdict})"
    )


def write_results(results):
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    trials = [
        {"name": trial_label(index, summary["batch_size"]), **summary}
        for index, summary in enumerate(results)
    ]
    speedup_ratio = None
    if len(results) == 2 and all(r["status"] == "success" for r in results):
        speedup_ratio = round(
            results[1]["elapsed_seconds"] / results[0]["elapsed_seconds"], 3
        )
    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "speedup_ratio": speedup_ratio,
        "trials": trials,
    }
    with open(RESULTS_DIR / "benchmark_results.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
        f.write("\n")


def generate_chart(results):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    labels = [trial_label(index, r["batch_size"]) for index, r in enumerate(results)]
    times = [r["elapsed_seconds"] for r in results]
    fig, ax = plt.subplots(figsize=(7, 5))
    bars = ax.bar(labels, times, width=0.55, color=["#1f77b4", "#d62728"])
    ax.bar_label(bars, fmt="%.1f s", padding=3)
    ax.set_ylabel("Total Time (seconds)")
    ax.set_title("Batch vs Individual Write Latency")
    ax.legend(bars, labels, loc="upper left")
    ax.grid(axis="y", alpha=0.3)
    ax.set_axisbelow(True)
    fig.tight_layout()
    fig.savefig(RESULTS_DIR / "latency_comparison.png", dpi=150)
    plt.close(fig)


def run_trial(batch_size):
    compose = DockerCompose(COMPOSE_FILE)
    check_services_running(compose)
    conn = connect_db(POSTGRES_DSN)
    batch_count = None
    inserted_total = 0
    avg_insert_ms = None
    try:
        stop_services(compose)
        truncate_table(conn)
        reset_consumer_offsets(compose, TOPIC, GROUP_ID)
        start_consumer(compose, batch_size)
        started = time.monotonic()
        produce_events(BOOTSTRAP_SERVERS, TOPIC, EVENT_COUNT)
        rows, elapsed = wait_for_rows(
            conn, EVENT_COUNT, started, TIMEOUT_SECONDS, POLL_INTERVAL_SECONDS
        )
        if rows >= EVENT_COUNT:
            time.sleep(SETTLE_SECONDS)
            batch_count, inserted_total, durations_ms = count_batches_from_logs(compose)
            if durations_ms:
                avg_insert_ms = sum(durations_ms) / len(durations_ms)
    finally:
        restore_services(compose, batch_size)
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
        if avg_insert_ms is not None:
            logger.info("Average batch INSERT time: %.2f ms", avg_insert_ms)
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
            "batch_size": batch_size,
            "elapsed_seconds": round(elapsed, 3),
            "events_per_second": round(events_per_second, 2),
            "batch_count": batch_count,
            "rows_committed": rows,
            "avg_batch_insert_ms": (
                round(avg_insert_ms, 2) if avg_insert_ms is not None else None
            ),
        }
        print(json.dumps(summary))
        return summary
    logger.error(
        f"Only {rows:,}/{EVENT_COUNT:,} rows committed after "
        f"{format_timeout_label(TIMEOUT_SECONDS)}"
    )
    summary = {
        "status": "timeout",
        "events": EVENT_COUNT,
        "batch_size": batch_size,
        "elapsed_seconds": round(elapsed, 3),
        "events_per_second": None,
        "batch_count": None,
        "rows_committed": rows,
        "avg_batch_insert_ms": None,
    }
    print(json.dumps(summary))
    return summary


if __name__ == "__main__":
    main()
