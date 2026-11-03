# app/core/model.py
# Loads autoencoder.onnx, scaler.pkl and threshold.json once (cached) and exposes
# score(df) -> reconstruction error per flow, per-feature errors, and anomaly flag.
# Must apply exactly the same cleaning, scaling and error formula as streaming/spark_processor.py,
# so the app and the Spark job give the same score for the same flow.
