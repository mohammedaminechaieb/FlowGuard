# =============================================================================
# File        : archive_raw_stream.py
# Module      : streaming
# Owner       : Amine
# Description : Second Spark Structured Streaming query that runs in parallel
#               with spark_processor.py. Consumes the same Kafka topic and
#               writes every raw flow record to HDFS as partitioned Parquet
#               (partitioned by date). The archive is used for offline
#               re-training and for re-scoring historical windows.
# Input       : Kafka topic  network-flows
#               .env  (KAFKA_BOOTSTRAP)
# Output      : HDFS path  /flowguard/archive/  (Parquet, partitioned by date)
# =============================================================================
# streaming/archive_raw_stream.py
# Second streaming query: Kafka -> raw flows as Parquet on HDFS (archive used for training and re-scoring).
