#!/usr/bin/env bash
# Run from the project root. SPARK_VERSION must match the Spark version of the image.
set -e
SPARK_VERSION="${SPARK_VERSION:-3.5.1}"
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:${SPARK_VERSION} \
  streaming/spark_processor.py