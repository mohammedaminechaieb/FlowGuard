#!/usr/bin/env bash
# =============================================================================
# File        : submit_spark.sh
# Module      : scripts
# Owner       : Mariem
# Description : Wrapper around spark-submit that packages the Kafka connector
#               JAR automatically using the --packages flag. The Spark version
#               is read from the SPARK_VERSION environment variable (default
#               3.5.1) so the script works with any compatible Spark image
#               without editing the file.
#               Run from the project root.
# Input       : SPARK_VERSION env var (optional, default 3.5.1)
#               streaming/spark_processor.py
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