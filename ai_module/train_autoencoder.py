# =============================================================================
# File        : train_autoencoder.py
# Module      : ai_module
# Owner       : Eya
# Description : Fits a StandardScaler on the benign training split, then trains
#               a PyTorch autoencoder (encoder-decoder) on those scaled flows.
#               The anomaly threshold is chosen as the 99th-percentile MSE on
#               the benign validation split so that all attack days are unseen
#               at threshold-selection time. Exports three artefacts used by the
#               Spark streaming job.
# Input       : data/parquet/train.parquet
#               data/parquet/validation.parquet
# Output      : ai_module/artifacts/autoencoder.onnx
#               ai_module/artifacts/scaler.pkl
#               ai_module/artifacts/threshold.json
# =============================================================================
# ai_module/train_autoencoder.py
# Fit StandardScaler on benign train, train the PyTorch autoencoder, choose the threshold from benign validation error, export autoencoder.onnx, scaler.pkl and threshold.json to ai_module/artifacts/.
