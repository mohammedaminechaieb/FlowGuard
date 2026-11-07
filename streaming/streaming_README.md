# streaming/

Real-time pipeline: Kafka producer, Spark Structured Streaming jobs, Cassandra schema, latency report.

## Files

| File | Owner | Role |
|---|---|---|
| `kafka_producer.py` | **Eya** | Replay CICIDS2017 Parquet rows as JSON into Kafka (topic `network-telemetry`); adds a `send_ts` timestamp per message |
| `spark_consumer.py` | **Eya** | Kafka → 78 features → scaler + ONNX autoencoder → MSE vs threshold → Cassandra: every flow to `all_flows`, anomalies to `alerts`, one summary row per batch to `batch_metrics` |
| `archive_raw_stream.py` | **Amine** | Second Spark streaming job: Kafka → raw messages as Parquet on HDFS, partitioned by date |
| `cassandra_setup.py` | **Mariem** | Create keyspace `flowguard` and tables `all_flows`, `alerts`, `batch_metrics` (run automatically by `cassandra-init`) |
| `latency_report.py` | **Mariem** | Read latency and batch metrics from Cassandra, compute p50/p95/p99 latency and throughput, write to `results/` |
| `test_insert.py` | **Mariem** | Smoke test: insert one row into `alerts` and read it back |
| `sampled_cassandra_data.cql` | **Mariem** | Sample rows to test Grafana panels without running the stream (`cqlsh -f`) |
| `requirements_streaming.txt` | **Amine** | Python dependencies for the scripts run on your PC |

## How to run

```bash
pip install -r streaming/requirements_streaming.txt
docker compose -f docker/docker-compose.yml up -d          # Kafka, Cassandra (+ schema), Spark, Grafana
python streaming/kafka_producer.py --dataset wednesday     # separate terminal
python streaming/latency_report.py --minutes 30            # after the stream has run