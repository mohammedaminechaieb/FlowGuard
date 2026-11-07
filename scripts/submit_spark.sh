#!/usr/bin/env bash
# =============================================================================
# File        : submit_spark.sh
# Module      : scripts
# Owner       : Mariem
# Description : Wrapper around spark-submit for running the FlowGuard jobs
#               outside Docker. Adds the Kafka and Cassandra connectors with
#               --packages and sets the Cassandra host. Run from the project root.
# Input       : SPARK_VERSION env var (optional, default 3.5.3)
#               CASSANDRA_HOST env var (optional, default localhost)
#               job file as first argument (default streaming/spark_consumer.py)
# Output      : running Spark Structured Streaming job
# =============================================================================
set -euo pipefail
# Usage: bash scripts/submit_spark.sh                                 (inference job)
#        bash scripts/submit_spark.sh streaming/archive_raw_stream.py  (archive job)
SPARK_VERSION="${SPARK_VERSION:-3.5.3}"
JOB="${1:-streaming/spark_consumer.py}"
spark-submit \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:${SPARK_VERSION},com.datastax.spark:spark-cassandra-connector_2.12:3.5.1" \
  --conf "spark.cassandra.connection.host=${CASSANDRA_HOST:-localhost}" \
  "$JOB"