# app/pages/4_Model_Results.py
# Evaluation results, read from the results/ folder written by ai_module/evaluate.py.
# Shows: recall per attack family, false-positive rate, ROC-AUC and PR-AUC,
# comparison of the autoencoder with Isolation Forest, One-Class SVM and Random Forest,
# reconstruction-error histogram benign vs attack with the threshold line,
# and the latency and throughput tables from streaming/latency_report.py.
