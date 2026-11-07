# =============================================================================
# File        : latency_report.py
# Module      : streaming
# Owner       : Amine
# Description : End-to-end latency and throughput report for the stream.
#               - Latency: per-event latency_ms (producer send -> Spark score)
#                 read from flowguard.all_flows -> p50 / p95 / p99 / mean / max
#               - Throughput: from flowguard.batch_metrics
#                   sustained  = flows / wall-clock time between batches
#                   processing = flows / time Spark spent processing
# Input       : Cassandra flowguard.all_flows, flowguard.batch_metrics
#               env CASSANDRA_HOST, CASSANDRA_PORT, CASSANDRA_KEYSPACE
# Output      : results/latency_report.csv
#               results/latency_distribution.png
# Usage       : py streaming/latency_report.py --minutes 30
# =============================================================================

import argparse
import csv
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
from cassandra.cluster import Cluster

BASE_DIR = Path(__file__).resolve().parent.parent
RESULTS_DIR = BASE_DIR / "results"

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "flowguard")

# Dataset names used by kafka_producer.py (= partition keys of all_flows).
DATASETS = ["train", "validation", "tuesday", "wednesday", "thursday", "friday"]


def parse_args():
    parser = argparse.ArgumentParser(description="FlowGuard latency / throughput report")
    parser.add_argument("--minutes", type=int, default=None,
                        help="Only use the last N minutes (default: everything)")
    parser.add_argument("--datasets", nargs="+", default=DATASETS,
                        help="Datasets (partitions) to include")
    return parser.parse_args()


def fetch_latencies(session, datasets, since):
    query = session.prepare(
        "SELECT latency_ms FROM all_flows WHERE dataset = ? AND kafka_timestamp >= ?"
    )
    values = []
    for dataset in datasets:
        for row in session.execute(query, (dataset, since)):
            if row.latency_ms is not None and np.isfinite(row.latency_ms):
                values.append(row.latency_ms)
    return np.asarray(values, dtype=float)


def fetch_batches(session, since):
    rows = session.execute(
        "SELECT batch_time, flows, processing_s FROM batch_metrics "
        "WHERE pipeline = 'spark' AND batch_time >= %s",
        (since,),
    )
    return sorted(rows, key=lambda r: r.batch_time)


def throughput(batches):
    total_flows = sum(b.flows or 0 for b in batches)
    processing_s = sum(b.processing_s or 0 for b in batches)
    sustained = None
    if len(batches) >= 2:
        wall_s = (batches[-1].batch_time - batches[0].batch_time).total_seconds()
        if wall_s > 0:
            sustained = total_flows / wall_s
    capacity = total_flows / processing_s if processing_s > 0 else None
    return total_flows, sustained, capacity


def save_plot(latencies, p50, p95, p99, path):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("[INFO] matplotlib not installed: no figure written.")
        return

    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.hist(latencies, bins=60, color="#4C72B0", alpha=0.85)
    for value, name, color in ((p50, "p50", "green"), (p95, "p95", "orange"), (p99, "p99", "red")):
        ax.axvline(value, color=color, linestyle="--", label=f"{name} = {value:.0f} ms")
    ax.set_xlabel("End-to-end latency (ms): producer send -> Spark score")
    ax.set_ylabel("Number of flows")
    ax.set_title("FlowGuard - latency distribution")
    ax.legend()
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"Figure saved -> {path}")


def main():
    args = parse_args()
    since = (datetime.now(timezone.utc) - timedelta(minutes=args.minutes)
             if args.minutes else datetime(1970, 1, 1, tzinfo=timezone.utc))

    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect(KEYSPACE)
    try:
        latencies = fetch_latencies(session, args.datasets, since)
        batches = fetch_batches(session, since)
    finally:
        cluster.shutdown()

    if latencies.size == 0:
        print("No latency data found. Send events with the updated producer "
              "(it adds send_ts), let Spark process them, then run this again.")
        return

    p50, p95, p99 = np.percentile(latencies, [50, 95, 99])
    total_flows, sustained, capacity = throughput(batches)

    report = [
        ("events_with_latency", int(latencies.size)),
        ("latency_p50_ms", round(float(p50), 2)),
        ("latency_p95_ms", round(float(p95), 2)),
        ("latency_p99_ms", round(float(p99), 2)),
        ("latency_mean_ms", round(float(latencies.mean()), 2)),
        ("latency_max_ms", round(float(latencies.max()), 2)),
        ("batches", len(batches)),
        ("flows_in_batches", total_flows),
        ("sustained_throughput_eps", None if sustained is None else round(sustained, 2)),
        ("processing_throughput_eps", None if capacity is None else round(capacity, 2)),
    ]

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = RESULTS_DIR / "latency_report.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["metric", "value"])
        writer.writerows(report)

    print("\nFlowGuard latency report")
    print("-" * 40)
    for name, value in report:
        print(f"{name:<28} {value}")
    print(f"\nCSV saved -> {csv_path}")

    save_plot(latencies, p50, p95, p99, RESULTS_DIR / "latency_distribution.png")


if __name__ == "__main__":
    main()