
# =============================================================================
# File        : spark_processor.py
# Module      : streaming
# Description : Main Spark Structured Streaming job for FlowGuard.
#
# Pipeline:
#   1. Read JSON messages from Kafka
#   2. Parse and cast event fields
#   3. Extract network flow features
#   4. Display received events
#
# Input:
#   Kafka topic: network-telemetry
#
# Output:
#   Spark console
# =============================================================================

from pathlib import Path
import os
import sys

import pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    size,
    map_entries,
    slice
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    MapType,
    DoubleType
)


# ============================================================
# 1. CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "network-telemetry"
)

BASE_DIR = Path(__file__).resolve().parent.parent

CHECKPOINT_DIR = (
    BASE_DIR / "checkpoints" / "spark_kafka_consumer"
)


# ============================================================
# 2. SPARK AND KAFKA CONNECTOR VERSIONS
# ============================================================

SPARK_VERSION = pyspark.__version__

SPARK_MAJOR_VERSION = int(SPARK_VERSION.split(".")[0])

if SPARK_MAJOR_VERSION >= 4:
    SCALA_VERSION = "2.13"
else:
    SCALA_VERSION = "2.12"

KAFKA_PACKAGE = (
    f"org.apache.spark:spark-sql-kafka-0-10_"
    f"{SCALA_VERSION}:{SPARK_VERSION}"
)


# ============================================================
# 3. JSON EVENT SCHEMA
# ============================================================

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("dataset", StringType(), True),

    StructField(
        "features",
        MapType(
            StringType(),
            DoubleType(),
            True
        ),
        True
    ),

    StructField("label", StringType(), True)
])


# ============================================================
# 4. INITIALIZE SPARK
# ============================================================

def create_spark_session():

    print("=" * 65)
    print("FLOWGUARD - SPARK STREAMING")
    print("=" * 65)

    print(f"Spark version : {SPARK_VERSION}")
    print(f"Kafka server  : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka topic   : {KAFKA_TOPIC}")
    print(f"Kafka package : {KAFKA_PACKAGE}")

    print("=" * 65)

    spark = (
        SparkSession.builder
        .appName("FlowGuard-Spark-Processor")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            KAFKA_PACKAGE
        )
        .config(
            "spark.sql.shuffle.partitions",
            "3"
        )
        .config(
            "spark.sql.streaming.forceDeleteTempCheckpointLocation",
            "false"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("[SUCCESS] Spark session initialized.")

    return spark


# ============================================================
# 5. READ EVENTS FROM KAFKA
# ============================================================

def read_from_kafka(spark):

    print("[INFO] Connecting to Kafka...")

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS
        )
        .option(
            "subscribe",
            KAFKA_TOPIC
        )
        .option(
            "startingOffsets",
            "earliest"
        )
        .option(
            "failOnDataLoss",
            "false"
        )
        .option(
            "maxOffsetsPerTrigger",
            "500"
        )
        .load()
    )

    print("[SUCCESS] Kafka stream configured.")

    return kafka_stream


# ============================================================
# 6. PARSE JSON EVENTS
# ============================================================

def parse_events(kafka_stream):

    print("[INFO] Parsing Kafka messages...")

    parsed_stream = (
        kafka_stream
        .select(
            col("timestamp").alias("kafka_timestamp"),
            col("partition"),
            col("offset"),

            from_json(
                col("value").cast("string"),
                EVENT_SCHEMA
            ).alias("event")
        )
        .select(
            "kafka_timestamp",
            "partition",
            "offset",

            col("event.event_id").alias("event_id"),
            col("event.dataset").alias("dataset"),
            col("event.features").alias("features"),
            col("event.label").alias("label")
        )
    )

    return parsed_stream


# ============================================================
# 7. DISPLAY RECEIVED EVENTS
# ============================================================

def display_events(parsed_stream):

    print("[INFO] Preparing console output...")

    display_stream = (
        parsed_stream
        .select(
            "kafka_timestamp",
            "partition",
            "offset",
            "event_id",
            "dataset",
            "label",

            size(
                col("features")
            ).alias("feature_count"),

            slice(
                map_entries(col("features")),
                1,
                5
            ).alias("first_5_features")
        )
    )

    query = (
        display_stream.writeStream
        .format("console")
        .outputMode("append")
        .option(
            "truncate",
            "false"
        )
        .option(
            "numRows",
            "5"
        )
        .option(
            "checkpointLocation",
            str(CHECKPOINT_DIR)
        )
        .trigger(
            processingTime="5 seconds"
        )
        .start()
    )

    print("[SUCCESS] Spark Streaming started.")
    print("[INFO] Waiting for Kafka events...")

    return query


# ============================================================
# 8. MAIN
# ============================================================

def main():

    spark = None
    query = None

    try:

        # Initialize Spark
        spark = create_spark_session()

        # Read Kafka stream
        kafka_stream = read_from_kafka(spark)

        # Parse JSON messages
        parsed_stream = parse_events(kafka_stream)

        # Display events
        query = display_events(parsed_stream)

        # Keep streaming alive
        query.awaitTermination()

    except KeyboardInterrupt:

        print("\n[INFO] Stopping FlowGuard Spark processor...")

    except Exception as error:

        print(f"\n[ERROR] Spark processing failed: {error}")

        raise

    finally:

        if query is not None and query.isActive:

            query.stop()

        if spark is not None:

            spark.stop()

            print("[INFO] Spark session stopped.")

        print("[INFO] FlowGuard processor terminated.")


# ============================================================
# 9. ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()
