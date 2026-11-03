# app/core/kafka_client.py
# Kafka access for the app: send one flow or a stream of rows as JSON to the network-flows topic,
# adding a send_ts timestamp (used for the latency measurement). Includes start and stop of a
# background replay thread with a configurable rate, and a health check.
