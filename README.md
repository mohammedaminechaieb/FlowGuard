# FlowGuard

Real-time network intrusion and zero-day anomaly detection with Kafka, Spark Structured Streaming and an autoencoder exported to ONNX.

Big Data project (II3).

## Overview

A Python producer replays recorded CICIDS2017 flow records into Kafka. A Spark Structured Streaming job scales each flow, scores it with an autoencoder (ONNX), and flags the flows whose reconstruction error (MSE) is above a threshold. Flagged flows and per-batch metrics are written to Cassandra and shown in Grafana. Raw flows are archived to HDFS as Parquet.

The autoencoder is trained offline only on benign traffic (Monday capture). Every attack in the test days is therefore "unseen during training". The threshold is chosen from benign validation data only.

## Architecture

    CICIDS2017 CSV -> kafka_producer.py -> Kafka -> Spark Structured Streaming (ONNX autoencoder) -> Cassandra -> Grafana
                                             |
                                             +-> Spark -> HDFS (Parquet archive) -> offline training and evaluation

## Technologies

| Role | Tool |
|---|---|
| Dataset | CICIDS2017 |
| Ingestion | Kafka (KRaft mode) |
| Stream processing | Spark 3.5 Structured Streaming (PySpark, micro-batch) |
| Model | PyTorch autoencoder, exported to ONNX, scored with onnxruntime in a Pandas UDF |
| Baselines | Isolation Forest, One-Class SVM, Random Forest (offline) |
| Archive | HDFS with Parquet |
| Alert store (NoSQL) | Cassandra 5.0 |
| Dashboard | Grafana 10.4 |
| Deployment | Docker Compose |

## Repository structure

    docker/       docker-compose.yml (Kafka, Cassandra, Grafana) and the exported Grafana dashboard
    ai_module/    data preparation, training, baselines, evaluation, ONNX artifacts
    streaming/    Kafka producer, Spark jobs, Cassandra setup, latency report
    scripts/      spark-submit helper
    data/         CICIDS2017 CSV files (not committed)
    results/      evaluation tables and figures
    docs/         presentation and signed AI-usage declaration

## Prerequisites

- Docker Desktop
- Python 3.10 or 3.11
- The spark-hadoop image (Spark 3.5.x and HDFS). Check with `spark-submit --version`.
- CICIDS2017 CSV files from https://www.unb.ca/cic/datasets/ids-2017.html, placed in `data/`
  (Monday for training, Wednesday and Friday for the simulated attack stream)

## Quick start

1. Configuration

       Copy-Item .env.example .env

2. Start Kafka, Cassandra and Grafana

       docker compose -f docker/docker-compose.yml up -d

3. Create the Cassandra schema (wait ~30s for Cassandra to be ready first)

       pip install -r streaming/requirements_streaming.txt
       python streaming/cassandra_setup.py

4. Prepare the data and train the model

       pip install -r ai_module/requirements_ai.txt
       python ai_module/prepare_data.py
       python ai_module/train_autoencoder.py

   This produces `autoencoder.onnx`, `scaler.pkl` and `threshold.json` in `ai_module/artifacts/`.

5. Start the Spark streaming job (on the Spark image, with SPARK_VERSION matching it)

       bash scripts/submit_spark.sh

6. Start the producer

       python streaming/kafka_producer.py

7. Open Grafana at http://localhost:3000 (admin / admin), install the Cassandra plugin
   (`hadesarchitect-cassandra-datasource`) and import `docker/grafana/flowguard_dashboard.json`.

If Spark runs inside a container, attach it to the `flowguard-net` network and use `kafka:29092` and `cassandra:9042` in `.env`.

## What we measure

- Detection per attack family (recall) and false-positive rate on benign flows
- End-to-end latency (p50, p95, p99) and sustained throughput
- Comparison with Isolation Forest and One-Class SVM (offline) and a Random Forest upper bound

## Limitations

- The input is a replay of flow records recorded in 2017, not live packet capture.
- CICIDS2017 has documented labelling and flow-generation errors.
- "Zero-day" here means "not seen during training".
- Cassandra and Grafana run without authentication for the demo.

## Team and work split

| Member | Responsibility |
|---|---|
| Amine | Data preparation, Autoencoder Training, HDFS archive, Docker infrastructure |
| Eya | Kafka Producer, ONNX export, baselines, Spark streaming job, Cassandra setup |
| Mariem | Evaluation, latency report, Grafana dashboard, spark-submit wrapper |

## Demo and documents

- Demo video: (link)
- Presentation: `docs/Presentation_Projet_II3.pdf`
- AI usage declaration: `docs/Declaration_IA_Signee.pdf`