# =============================================================================
# File        : evaluate.py
# Module      : ai_module
# Owner       : Mariem
# Description : Loads the test days (Wednesday and Friday attacks) and scores
#               them with the autoencoder, Isolation Forest, One-Class SVM and
#               Random Forest. Computes per-attack-family recall, false-positive
#               rate, ROC-AUC and PR-AUC. Repeats over several random seeds and
#               reports mean ± std. Writes summary tables (CSV) and figures
#               (PNG) to results/.
# Input       : data/parquet/test_wednesday.parquet
#               data/parquet/test_friday.parquet
#               ai_module/artifacts/autoencoder.onnx
#               ai_module/artifacts/scaler.pkl
#               ai_module/artifacts/threshold.json
#               ai_module/artifacts/iforest.pkl
#               ai_module/artifacts/ocsvm.pkl
#               ai_module/artifacts/rf.pkl
# Output      : results/metrics_table.csv
#               results/roc_curves.png
#               results/pr_curves.png
# =============================================================================
# ai_module/evaluate.py
# Per-attack-family recall, false-positive rate, ROC-AUC and PR-AUC; mean and std over several seeds; write tables and figures to results/.
