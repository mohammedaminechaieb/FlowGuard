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

import os
import json
import numpy as np
import pandas as pd
import onnxruntime as ort
import pickle
import pyarrow.parquet as pq
from sklearn.metrics import (precision_score, 
                            recall_score,f1_score, fbeta_score,
                            roc_auc_score, average_precision_score, 
                            confusion_matrix)

DATA_DIR = os.environ.get("DATA_DIR", "data")
PARQUET_DIR = os.path.join(DATA_DIR, "parquet")
ARTIFACTS_DIR = os.environ.get("ARTIFACTS_DIR", "ai_module/artifacts")
RESULTS_DIR = "results"

TEST_FILES = ["test_tuesday.parquet", 
            "test_wednesday.parquet", 
            "test_thursday.parquet", 
            "test_friday.parquet"]
BATCH = 50_000

def load_artifacts():
    with open(os.path.join(ARTIFACTS_DIR, "scaler.pkl"), "rb") as f:
        scaler = pickle.load(f)
    with open(os.path.join(ARTIFACTS_DIR, "threshold.json"), "r") as f:
        threshold = json.load(f)["threshold"]
    session = ort.InferenceSession(
        os.path.join(ARTIFACTS_DIR, "autoencoder.onnx"),
        providers = ["CPUExecutionProvider"]
    )
    return scaler, threshold, session

def feature_columns():
    names = pq.read_schema(os.path.join(PARQUET_DIR, TEST_FILES[0])).names
    return [name for name in names 
            if name !="Label" and not name.startswith("__")]
    
def score(X, scaler, session):
    """Reconstruction error (MSE) per flow, computed with the ONNX model."""
    Xs = scaler.transform(X).astype(np.float32)
    scores = []
    for i in range(0, len(Xs), BATCH):
        chunk = Xs[i:i + BATCH]
        recon = session.run(None, {"input": chunk})[0]
        scores.append(np.mean((chunk - recon) ** 2, axis=1))
    return np.concatenate(scores)

def sanity_check(scaler, threshold, session, cols):
    val = pd.read_parquet(os.path.join(PARQUET_DIR, "validation.parquet"))
    mse = score(val[cols].values.astype(np.float32), scaler, session)
    rate = (mse > threshold).mean()
    print(f"  Validation flows above threshold: {rate:.2%}  (expected about 1%)")
    
def load_test(cols): 
    df = pd.concat([pd.read_parquet(os.path.join(PARQUET_DIR, f)) 
                    for f in TEST_FILES], ignore_index=True)
    X = df[cols].values.astype(np.float32)
    labels = df["Label"].astype(str)
    return X, labels

def evaluate(mse, labels, threshold):
    is_benign = labels == "BENIGN"
    y_true = (~is_benign).astype(int).values
    y_pred = (mse > threshold).astype(int)
    
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()
    results = {
        "threshold": float(threshold),
        "flows" : int(len(y_true)),
        "attack_flows" : int(y_true.sum()),
        "precision" : float(precision_score(y_true, y_pred)),
        "recall" : float(recall_score(y_true, y_pred)),
        "f1" : float(f1_score(y_true, y_pred)),
        "f2" : float(fbeta_score(y_true, y_pred, beta=2)),
        "false_positive_rate" : float(fp / (fp + tn)),
        "roc_auc" : float(roc_auc_score(y_true, mse)),
        "pr_auc" : float(average_precision_score(y_true, mse)),
        "confusion_matrix" : {
            "tn": int(tn),
            "fp": int(fp),
            "fn": int(fn),
            "tp": int(tp)
        },   
    }
    
    attacks = pd.DataFrame({"Label": labels[y_true == 1].values, 
                            "flagged": y_pred[y_true == 1]})
    per_attack = (attacks.groupby("Label")["flagged"]
                .agg(recall="mean", flows="count").sort_values("recall"))
    return results, per_attack

def main():
    print("=" * 60)
    print("FlowGuard - evaluate.py")
    print("=" * 60)
    
    scaler, threshold, session = load_artifacts()
    cols = feature_columns()
    
    print("Sanity check ...")
    sanity_check(scaler, threshold, session, cols)
    
    print('Loading test data ...')
    X, labels = load_test(cols)
    mse = score(X, scaler, session)
    
    print("Scoring test data ...")
    results, per_attack = evaluate(mse, labels, threshold)
    
    for key, value in results.items():
        print(f"{key:22s}: {value}")
    print("\nPer-attack-family recall:")
    print(per_attack.to_string())
    
    with open(os.path.join(RESULTS_DIR, "metrics_table.json"), "w") as f:    
        json.dump(results, f, indent=2)
    per_attack.to_csv(os.path.join(RESULTS_DIR, "per_attack_recall.csv"))
    print(f"\nResults written to {RESULTS_DIR}/metrics_table.json and per_attack_recall.csv")
    
if __name__ == "__main__":
    main()
    