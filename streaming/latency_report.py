# =============================================================================
# File        : latency_report.py
# Module      : streaming
# Owner       : Mariem
# Description : Reads the flowguard-metrics Elasticsearch index, computes
#               end-to-end latency statistics (p50, p95, p99) using the
#               send_ts producer timestamp and the ES index timestamp, and
#               measures sustained throughput (records/second). Writes a
#               summary CSV and a latency distribution figure to results/.
# Input       : Elasticsearch index  flowguard-metrics
#               .env  (ES_URL, ES_METRICS_INDEX)
# Output      : results/latency_report.csv
#               results/latency_distribution.png
# =============================================================================
# streaming/latency_report.py
# Read flowguard-metrics, compute p50/p95/p99 latency and sustained throughput, write to results/.
