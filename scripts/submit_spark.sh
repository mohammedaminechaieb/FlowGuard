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
set -e
SPARK_VERSION=""
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12: \
  streaming/spark_processor.py#!/usr/bin/env bash
# Run from the project root. SPARK_VERSION must match the Spark version of the image.
set -e
SPARK_VERSION="${SPARK_VERSION:-3.5.1}"
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:${SPARK_VERSION} \
  streaming/spark_consumer.py