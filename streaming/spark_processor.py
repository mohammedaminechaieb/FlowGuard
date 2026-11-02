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
#                      - bulk-index flagged flows to flowguard-alerts
#                      - bulk-index batch metrics to flowguard-metrics
#                    Both use a deterministic _id (batch_id + Kafka offset)
#                    so re-runs are idempotent.
# Input       : Kafka topic  network-flows
#               ai_module/artifacts/autoencoder.onnx
#               ai_module/artifacts/scaler.pkl
#               ai_module/artifacts/threshold.json
#               .env  (KAFKA_BOOTSTRAP, ES_URL, ...)
# Output      : Elasticsearch index  flowguard-alerts  (flagged flows)
#               Elasticsearch index  flowguard-metrics (per-batch stats)
# =============================================================================
# streaming/spark_processor.py
# Kafka -> parse JSON -> scale -> ONNX Pandas UDF -> MSE vs threshold -> foreachBatch: bulk-write flagged flows and batch metrics to Elasticsearch with deterministic _id.
