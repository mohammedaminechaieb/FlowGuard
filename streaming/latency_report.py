# =============================================================================
# File        : latency_report.py
# Module      : streaming
# Owner       : Mariem
# Description : Reads the flowguard.metrics Cassandra table, computes
#               end-to-end latency statistics (p50, p95, p99) using the
#               send_ts producer timestamp and the Cassandra write timestamp,
#               and measures sustained throughput (records/second). Writes a
#               summary CSV and a latency distribution figure to results/.
# Input       : Cassandra table  flowguard.metrics
#               .env  (CASSANDRA_HOST, CASSANDRA_KEYSPACE)
# Output      : results/latency_report.csv
#               results/latency_distribution.png
# =============================================================================