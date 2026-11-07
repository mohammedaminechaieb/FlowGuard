# scripts/

Helper scripts for running the project.

## Files

| File | Owner | Role |
|---|---|---|
| `submit_spark.sh` | **Amine** | `spark-submit` wrapper for running the jobs outside Docker — adds the Kafka and Cassandra connectors |

## Usage

```bash
bash scripts/submit_spark.sh                                   # inference job (default)
bash scripts/submit_spark.sh streaming/archive_raw_stream.py   # archive job
```
