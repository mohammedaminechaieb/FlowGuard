# FlowGuard — Work Partition

Every member contributes to all modules. The table below shows who owns which file inside each module.

---

## Amine — Data & Infrastructure

| Module | File | Task |
|---|---|---|
| `ai_module` | `prepare_data.py` | Load CICIDS2017, clean, split, write Parquet |
| `ai_module` | `requirements_ai.txt` | AI module dependencies |
| `streaming` | `train_autoencoder.py` | Train PyTorch autoencoder, export ONNX + scaler + threshold |
| `ai_module` | `evaluate.py` | Per-attack recall, FPR, ROC-AUC, PR-AUC; tables and figures |
| `streaming` | `archive_raw_stream.py` | Archive raw flows to HDFS as Parquet |
| `streaming` | `requirements_streaming.txt` | Streaming module dependencies |
| `docker` | `docker-compose.yml` | Kafka, Cassandra, Grafana services |

---

## Eya — Modelling & Stream Processing

| Module | File | Task |
|---|---|---|
| `ai_module` | `kafka_producer.py` | Replay flows into Kafka with timestamps |
| `ai_module` | `train_baselines.py` | Isolation Forest, One-Class SVM, Random Forest baselines |
| `streaming` | `spark_processor.py` | Main Spark job: score flows with ONNX, write alerts + metrics to Cassandra |

---

## Mariem — Evaluation, Reporting & Visualisation

| Module | File | Task |
|---|---|---|
| `streaming` | `cassandra_setup.py` | Create Cassandra keyspace and tables |
| `streaming` | `latency_report.py` | p50/p95/p99 latency and throughput report |
| `scripts` | `submit_spark.sh` | spark-submit wrapper |
| `docker` | `grafana/flowguard_dashboard.json` | Grafana dashboard |

---

## Shared responsibilities

| Task | Who |
|---|---|
| `README.md` (main) | All three |
| `docs/` (presentation, AI declaration) | All three |
| Integration testing | All three |
| Demo video | All three |

---

## Dependency order

```
Amine: prepare_data.py ──────────────────────────────────────────────┐
                                                                      ▼
Eya:   train_autoencoder.py → artifacts/ ──► spark_processor.py ──► results/
       train_baselines.py ─────────────────────────────────────────► evaluate.py (Mariem)
                                                                      ▲
Amine: docker-compose up → kafka_producer.py ────────────────────────┘
Eya:   cassandra_setup.py
Mariem: latency_report.py (after stream runs)
```