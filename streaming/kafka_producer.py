# =============================================================================
# File        : kafka_producer.py
# Module      : streaming
# Owner       : Eya
# Description : Reads CICIDS2017 CSV rows and replays them as JSON messages
#               into a Kafka topic at a configurable rate (rows/second).
#               Adds a producer-side timestamp field (send_ts) to each message
#               so that end-to-end latency can be computed downstream.
# Input       : data/*.csv  (CICIDS2017 raw files)
#               .env        (KAFKA_BOOTSTRAP, KAFKA_TOPIC, DATA_DIR)
# Output      : Kafka topic  ->  network-flows  (JSON messages)
# =============================================================================
# streaming/kafka_producer.py
# Replay CICIDS2017 rows as JSON into Kafka at a configurable rate; add a producer timestamp (send_ts) to each message.
