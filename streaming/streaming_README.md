# streaming/

Real-time pipeline: Kafka producer, Spark Structured Streaming jobs, Cassandra setup, latency report.

## Files

| File | Owner | Role |
|---|---|---|
| `kafka_producer.py` | **Amine** | Replay CICIDS2017 rows as JSON into Kafka at a configurable rate; adds `send_ts` producer timestamp per message |
| `archive_raw_stream.py` | **Amine** | Second Spark streaming query: Kafka → raw flows as Parquet on HDFS (archive for training and re-scoring) |
| `cassandra_setup.py` | **Eya** | Create Cassandra keyspace and tables: `flowguard.alerts` and `flowguard.metrics` |
| `spark_processor.py` | **Eya** | Kafka → parse JSON → scale → ONNX Pandas UDF → MSE vs threshold → foreachBatch: write flagged flows and batch metrics to Cassandra |
| `latency_report.py` | **Mariem** | Read `flowguard.metrics`, compute p50/p95/p99 latency and sustained throughput, write to `results/` |
| `requirements_streaming.txt` | **Amine** | Python dependencies for this module |

## How to run

```bash
pip install -r streaming/requirements_streaming.txt
python streaming/cassandra_setup.py
bash scripts/submit_spark.sh
python streaming/kafka_producer.py      # separate terminal
python streaming/latency_report.py      # after the stream has run
```