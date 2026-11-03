# =============================================================================
# File        : spark_processor.py
# Module      : streaming
# Owner       : Eya
# Description : Main Spark Structured Streaming job.
#               Pipeline per micro-batch:
#                 1. Read JSON messages from Kafka
#                 2. Parse and cast all flow feature columns
#                 3. Scale features with the pre-fitted StandardScaler (scaler.pkl)
#                 4. Score with the ONNX autoencoder via a Pandas UDF
#                    (onnxruntime inference inside the UDF)
#                 5. Flag flows whose MSE exceeds threshold.json
#                 6. foreachBatch:
#                      - write flagged flows to Cassandra flowguard.alerts
#                      - write batch metrics to Cassandra flowguard.metrics
#                    Partition key includes time bucket to avoid hot partitions.
# Input       : Kafka topic  network-flows
#               ai_module/artifacts/autoencoder.onnx
#               ai_module/artifacts/scaler.pkl
#               ai_module/artifacts/threshold.json
#               .env  (KAFKA_BOOTSTRAP, CASSANDRA_HOST, CASSANDRA_KEYSPACE)
# Output      : Cassandra table  flowguard.alerts  (flagged flows)
#               Cassandra table  flowguard.metrics  (per-batch stats)
# =============================================================================