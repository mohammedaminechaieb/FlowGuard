"""
Référence hors-ligne pour valider le traitement fait par Spark.

Rejoue exactement les mêmes étapes que run_inference() (spark_consumer.py)
sur les N premières lignes d'un fichier Parquet : sélection des 78 features
dans l'ordre de feature_columns.json, scaler, autoencodeur ONNX, MSE, seuil.

Les event_id affichés sont ceux du producer (<dataset>_<index>) : on peut donc
comparer, ligne par ligne, avec la sortie de Spark.

Usage (depuis la racine du projet, venv activé) :
    python tests/check_offline.py --dataset wednesday --n 20
"""

import argparse
import json
import sys
from pathlib import Path

import joblib
import numpy as np
import onnxruntime as ort
import pyarrow.parquet as pq

BASE_DIR = Path(__file__).resolve().parent.parent
PARQUET_DIR = BASE_DIR / "data" / "parquet"

DATASETS = {
    "train": "train.parquet",
    "validation": "validation.parquet",
    "tuesday": "test_tuesday.parquet",
    "wednesday": "test_wednesday.parquet",
    "thursday": "test_thursday.parquet",
    "friday": "test_friday.parquet",
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", choices=list(DATASETS), default="wednesday")
    parser.add_argument("--n", type=int, default=20, help="Nombre de lignes à rejouer")
    parser.add_argument("--model-dir", default=str(BASE_DIR / "models"))
    parser.add_argument("--threshold", type=float, default=0.03222443163394928)
    args = parser.parse_args()

    model_dir = Path(args.model_dir)
    features = json.loads((model_dir / "feature_columns.json").read_text(encoding="utf-8"))

    parquet_file = pq.ParquetFile(PARQUET_DIR / DATASETS[args.dataset])
    columns = parquet_file.schema_arrow.names

    # 1. Les noms de features du modèle doivent exister dans le Parquet envoyé par le producer.
    missing = [name for name in features if name not in columns]
    extra = [name for name in columns if name != "Label" and name not in features]
    print(f"Features du modèle : {len(features)} | colonnes du Parquet : {len(columns)}")
    if missing:
        print(f"[ERREUR] Features absentes du Parquet (Spark recevrait des null) : {missing}")
        sys.exit(1)
    if extra:
        print(f"[INFO] Colonnes du Parquet non utilisées par le modèle : {extra}")

    # 2. Mêmes étapes que run_inference().
    batch = next(parquet_file.iter_batches(batch_size=args.n, columns=features + ["Label"]))
    df = batch.to_pandas()

    X = df[features].to_numpy(dtype=np.float32, copy=True)
    valid = np.isfinite(X).all(axis=1)

    scaler = joblib.load(str(model_dir / "scaler.pkl"))
    session = ort.InferenceSession(
        str(model_dir / "autoencoder.onnx"), providers=["CPUExecutionProvider"]
    )
    input_name = session.get_inputs()[0].name
    output_name = session.get_outputs()[0].name

    X_scaled = scaler.transform(X[valid]).astype(np.float32)
    reconstructed = session.run([output_name], {input_name: X_scaled})[0]
    mse = np.mean(np.square(X_scaled - reconstructed), axis=1)

    # 3. Affichage comparable à la sortie Spark.
    print(f"\n{'event_id':<20} {'label':<14} {'mse':<14} prediction")
    position = 0
    for i in range(len(df)):
        event_id = f"{args.dataset}_{i}"
        label = df["Label"].iloc[i]
        if not valid[i]:
            print(f"{event_id:<20} {label:<14} {'-':<14} IGNORÉ (NaN/inf : Spark écarte aussi cette ligne)")
            continue
        value = float(mse[position])
        prediction = "ANOMALY" if value > args.threshold else "BENIGN"
        print(f"{event_id:<20} {label:<14} {value:<14.6f} {prediction}")
        position += 1

    print(f"\n{int(valid.sum())} lignes scorées sur {len(df)} (seuil = {args.threshold})")


if __name__ == "__main__":
    main()