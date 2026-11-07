# =============================================================================
# File        : train_baselines.py
# Module      : ai_module
# Owner       : Amine
# Description : Trains three offline baseline models on the same benign
#               train/validation split used by the autoencoder:
#                 - Isolation Forest (unsupervised)
#                 - One-Class SVM   (unsupervised)
#                 - Random Forest   (supervised upper bound, uses attack labels)
#               Results are saved for comparison in evaluate.py.
# Input       : data/parquet/train.parquet
#               data/parquet/validation.parquet
#               data/parquet/test_wednesday.parquet  (attack labels for RF)
#               data/parquet/test_friday.parquet
# Output      : ai_module/artifacts/iforest.pkl
#               ai_module/artifacts/ocsvm.pkl
#               ai_module/artifacts/rf.pkl
# =============================================================================
# ai_module/train_baselines.py
# Offline baselines on the same split: Isolation Forest, One-Class SVM, and Random Forest as a supervised upper bound.

import os
import json
import time
import pickle

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.svm import OneClassSVM

DATA_DIR = os.environ.get("DATA_DIR", "data")
PARQUET_DIR = os.path.join(DATA_DIR, "parquet")
ARTIFACTS_DIR = os.environ.get("ARTIFACTS_DIR", "ai_module/artifacts")
BASELINE_DIR = os.path.join(ARTIFACTS_DIR, "baselines")

TEST_FILES = ["test_tuesday.parquet",
            "test_wednesday.parquet",
            "test_thursday.parquet",
            "test_friday.parquet"]

RANDOM_SEED = 42
THRESHOLD_PERCENTILE = 95 
RF_TRAIN_FRACTION = 0.5 # share of test flows used to train the RF
RF_MAX_SAMPLES = 200_000 # rows per tree (keeps training time reasonable)
OCSVM_TRAIN_SAMPLES = 10_000 # benign rows used to fit the One-Class SVM
BATCH = 50_000 

#shared helpers

def feature_columns():
    names = pq.read_schema(os.path.join(PARQUET_DIR, "train.parquet")).names
    return [n for n in names if n != "Label" and not n.startswith("__")]

def load_parquet(files, cols):
    df = pd.concat([pd.read_parquet(os.path.join(PARQUET_DIR, f)) for f in files],
                ignore_index=True)
    return df[cols].values.astype(np.float32), df["Label"].astype(str).reset_index(drop=True)

def rf_train_mask(n_rows):
    """Same random split of the test flows in train_baselines.py and evaluate.py:
    True = used to train the Random Forest, False = held out for evaluation."""
    return np.random.default_rng(RANDOM_SEED).random(n_rows) < RF_TRAIN_FRACTION

def load_scaler():
    with open(os.path.join(ARTIFACTS_DIR, "scaler.pkl"), "rb") as f:
        return pickle.load(f)

def baseline_scores(name, model, X_scaled):
    """Anomaly score per flow: higher = more anomalous (same convention as the MSE)."""
    out = []
    for i in range(0, len(X_scaled), BATCH):
        chunk = X_scaled[i:i + BATCH]
        if name == "iforest":
            out.append(-model.score_samples(chunk))
        elif name == "ocsvm":
            out.append(-model.decision_function(chunk))
        else:  # rf
            out.append(model.predict_proba(chunk)[:, 1])
    return np.concatenate(out)

# ---------------------------------------------------------------- 
# training

def main():
    print("=" * 60)
    print("FlowGuard - train_baselines.py")
    print("=" * 60)
    os.makedirs(BASELINE_DIR, exist_ok=True)
    rng = np.random.default_rng(RANDOM_SEED)

    scaler = load_scaler()
    cols = feature_columns()

    X_train, _ = load_parquet(["train.parquet"], cols)
    X_val, _ = load_parquet(["validation.parquet"], cols)
    X_train = scaler.transform(X_train).astype(np.float32)
    X_val = scaler.transform(X_val).astype(np.float32)
    print(f"Benign train: {X_train.shape}, validation: {X_val.shape}")

    models, thresholds = {}, {}

    # 1. Isolation Forest (benign only)
    t0 = time.time()
    print("\n[1/3] Isolation Forest ...")
    iforest = IsolationForest(n_estimators=200, random_state=RANDOM_SEED, n_jobs=-1)
    iforest.fit(X_train)
    models["iforest"] = iforest
    print(f"      done in {time.time() - t0:.0f} s")

    # 2. One-Class SVM (benign only, random subsample)
    t0 = time.time()
    print(f"[2/3] One-Class SVM (RBF, {OCSVM_TRAIN_SAMPLES:,} benign flows) ...")
    sub = rng.choice(len(X_train), size=min(OCSVM_TRAIN_SAMPLES, len(X_train)), replace=False)
    ocsvm = OneClassSVM(kernel="rbf", gamma="scale", nu=0.05)
    ocsvm.fit(X_train[sub])
    models["ocsvm"] = ocsvm
    print(f"      done in {time.time() - t0:.0f} s ({len(ocsvm.support_):,} support vectors)")

    # Thresholds of the unsupervised models: same rule as the autoencoder
    for name in ("iforest", "ocsvm"):
        val_scores = baseline_scores(name, models[name], X_val)
        thresholds[name] = float(np.percentile(val_scores, THRESHOLD_PERCENTILE))
        print(f"      {name} threshold (p{THRESHOLD_PERCENTILE} validation) = {thresholds[name]:.6f}")

    # 3. Random Forest (supervised upper bound, sees attack labels)
    t0 = time.time()
    print("[3/3] Random Forest (supervised upper bound) ...")
    X_test, labels = load_parquet(TEST_FILES, cols)
    mask = rf_train_mask(len(X_test))
    X_rf = scaler.transform(X_test[mask]).astype(np.float32)
    y_rf = (labels[mask] != "BENIGN").astype(int).values
    del X_test
    rf = RandomForestClassifier(
        n_estimators=100,
        max_depth=20,
        max_samples=min(RF_MAX_SAMPLES, len(X_rf)),
        class_weight="balanced_subsample",
        n_jobs=-1,
        random_state=RANDOM_SEED,
    )
    rf.fit(X_rf, y_rf)
    models["rf"] = rf
    thresholds["rf"] = 0.5
    print(f"      trained on {len(X_rf):,} test flows ({y_rf.sum():,} attacks) in {time.time() - t0:.0f} s")

    # Save
    for name, model in models.items():
        with open(os.path.join(BASELINE_DIR, f"{name}.pkl"), "wb") as f:
            pickle.dump(model, f)
    with open(os.path.join(BASELINE_DIR, "thresholds.json"), "w") as f:
        json.dump({"percentile": THRESHOLD_PERCENTILE, **thresholds}, f, indent=2)

    print(f"\nBaselines saved to {BASELINE_DIR}/ (iforest.pkl, ocsvm.pkl, rf.pkl, thresholds.json)")


if __name__ == "__main__":
    main()