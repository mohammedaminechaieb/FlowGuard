# streaming/spark_processor.py
# Kafka -> parse JSON -> scale -> ONNX Pandas UDF -> MSE vs threshold -> foreachBatch: bulk-write flagged flows and batch metrics to Elasticsearch with deterministic _id.
