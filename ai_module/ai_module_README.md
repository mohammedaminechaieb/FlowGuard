# ai_module/

Offline training pipeline: data preparation, autoencoder, baselines, evaluation.

## Files

| File | Owner | Role |
|---|---|---|
| `prepare_data.py` | **Amine** | Load CICIDS2017 CSVs, clean (inf/NaN, duplicates, column names), split Monday benign into train/validation, write Parquet to HDFS (or `data/parquet/` locally) |
| `train_autoencoder.py` | **Amine** | Fit StandardScaler on benign train, train PyTorch autoencoder, choose threshold from benign validation MSE, export `autoencoder.onnx`, `scaler.pkl`, `threshold.json` to `artifacts/` |
| `train_baselines.py` | **Amine** | Offline baselines on the same split: Isolation Forest, One-Class SVM, Random Forest (supervised upper bound) |
| `evaluate.py` | **Amine** | Per-attack-family recall, FPR, ROC-AUC, PR-AUC; mean and std over several seeds; write tables and figures to `results/` |
| `requirements_ai.txt` | **Amine** | Python dependencies for this module |
| `artifacts/` | generated | `autoencoder.onnx`, `scaler.pkl`, `threshold.json` — produced by training, not committed |

## How to run

```bash
pip install -r ai_module/requirements_ai.txt
python ai_module/prepare_data.py
python ai_module/train_autoencoder.py   # produces artifacts/
python ai_module/train_baselines.py
python ai_module/evaluate.py            # writes to results/
```
