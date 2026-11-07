# =============================================================================
# File        : archive_raw_stream.py
# Module      : streaming
# Owner       : Amine
# Description : Second Spark Structured Streaming job, running next to
#               spark_consumer.py. Reads the same Kafka topic and archives
#               every raw message as Parquet, partitioned by date, for
#               offline re-training and re-scoring.
#               Default target is a local folder; set ARCHIVE_PATH to an
#               hdfs:// URI to write to HDFS instead.
# Input       : Kafka topic network-telemetry
# Output      : ARCHIVE_PATH (default data/archive/date=YYYY-MM-DD/*.parquet)
# Read back   : spark.read.parquet(ARCHIVE_PATH)  or  pandas.read_parquet(...)
# =============================================================================

import os
import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

BASE_DIR = Path(__file__).resolve().parent.parent

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "network-telemetry")
ARCHIVE_PATH = os.getenv("ARCHIVE_PATH", str(BASE_DIR / "data" / "archive"))
CHECKPOINT_PATH = os.getenv(
    "ARCHIVE_CHECKPOINT_PATH", str(BASE_DIR / "checkpoints" / "archive_raw")
)
TRIGGER_INTERVAL = os.getenv("ARCHIVE_TRIGGER_INTERVAL", "30 seconds")

# Only the header fields become columns. The 78 features stay inside
# raw_json, so the archive keeps each message exactly as it was sent.
HEADER_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("dataset", StringType(), True),
    StructField("send_ts", DoubleType(), True),
    StructField("label", StringType(), True),
])

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FlowGuard-Archive")


def to_archive_rows(raw):
    return (
        raw.select(
            col("timestamp").alias("kafka_timestamp"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("value").cast("string").alias("raw_json"),
        )
        .withColumn("event", from_json(col("raw_json"), HEADER_SCHEMA))
        .select(
            "kafka_timestamp", "kafka_partition", "kafka_offset",
            col("event.event_id").alias("event_id"),
            col("event.dataset").alias("dataset"),
            col("event.label").alias("label"),
            col("event.send_ts").alias("send_ts"),
            "raw_json",
        )
        .withColumn("date", to_date(col("kafka_timestamp")))
    )


def main():
    spark = SparkSession.builder.appName("FlowGuard-Archive").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Archiving %s from %s to %s", KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS, ARCHIVE_PATH)

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    query = (
        to_archive_rows(raw).writeStream
        .queryName("flowguard_archive")
        .format("parquet")
        .option("path", ARCHIVE_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("date")
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()