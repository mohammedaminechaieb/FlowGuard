# =============================================================================
# File        : cassandra_setup.py
# Module      : streaming
# Owner       : Eya
# Description : Creates the Cassandra keyspace and tables used by the project
#               with explicit schemas so that Grafana can query them correctly.
#                 - flowguard.alerts  : one row per flagged (anomalous) flow
#                 - flowguard.metrics : one row per Spark micro-batch
#                   (batch_id, record count, mean MSE, p50/p95/p99 latency)
#               Partition key includes a time bucket to avoid unbounded
#               partitions for a single attacking host.
#               Safe to re-run: uses IF NOT EXISTS on all CREATE statements.
# Input       : .env  (CASSANDRA_HOST, CASSANDRA_KEYSPACE)
# Output      : Cassandra keyspace flowguard with tables alerts and metrics
# =============================================================================