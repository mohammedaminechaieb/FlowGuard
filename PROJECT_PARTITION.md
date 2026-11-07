# FlowGuard — Work Partition

Every member contributes to all modules. The table below shows who owns which file inside each module.

---

## Amine — Data, Model, Infrastructure & Automation

| Module | File | Task |
|---|---|---|
| `ai_module` | `prepare_data.py` | Load CICIDS2017, clean, split, write Parquet |
| `ai_module` | `train_autoencoder.py` | Train PyTorch autoencoder, export ONNX + scaler + threshold |
| `ai_module` | `evaluate.py` | Per-attack recall, FPR, ROC-AUC, PR-AUC, F1/F2 |
| `ai_module` | `artifacts/feature_columns.json` | Feature order used by Spark inference |
| `ai_module` | `requirements_ai.txt` | AI module dependencies |
| `streaming` | `archive_raw_stream.py` | Archive the raw stream to HDFS as Parquet |
| `streaming` | `latency_report.py` | p50/p95/p99 latency and throughput report |
| `scripts` | `submit_spark.sh` | spark-submit wrapper (outside Docker) |
| `docker` | `docker-compose.yml` | All services, automatic Cassandra schema (`cassandra-init`), HDFS archive job |
| `docker` | `Grafana/` | Grafana provisioning and automatic FlowGuard dashboard (dashboard as code) |
| `ai_module` | `train_baselines.py` | Isolation Forest, One-Class SVM, Random Forest baselines |

---

## Eya — Modelling & Stream Processing

| Module | File | Task |
|---|---|---|
| `streaming` | `kafka_producer.py` | Replay flows into Kafka with timestamps |
| `streaming` | `spark_consumer.py` | Main Spark job: score flows with ONNX, write flows, alerts and batch metrics to Cassandra |
| `docker` | `spark.Dockerfile` | Spark image (Python, Java, PySpark, model libraries) |
| `tests` | `check_offline.py` | Offline reference to validate Spark scores |

---

## Mariem — Cassandra & Grafana prototype

| Module | File | Task |
|---|---|---|
| `streaming` | `cassandra_setup.py` | Create Cassandra keyspace and tables |
| `streaming` | `test_insert.py` | Cassandra smoke test |
| `streaming` | `sampled_cassandra_data.cql` | Sample rows for testing Grafana |
| `streaming` | `requirements_streaming.txt` | Streaming module dependencies |
| `docker` | Grafana (UI) | First dashboard prototype built manually in Grafana, used to validate the Cassandra queries |

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
Amine: prepare_data.py ──► train_autoencoder.py ──► ai_module/artifacts/ ──┐
                                                                            ▼
Amine: docker compose up (cassandra-init ◄─ Mariem: cassandra_setup.py) ──► Eya: spark_consumer.py ──► Cassandra ──► Grafana (Mariem: prototype, Amine: dashboard as code)
Eya:   kafka_producer.py ──► Kafka ──┘                                     │
                                     └──► Amine: archive_raw_stream.py ──► HDFS
Amine: evaluate.py, latency_report.py ──► results/
Amine: train_baselines.py ──► evaluate.py
```