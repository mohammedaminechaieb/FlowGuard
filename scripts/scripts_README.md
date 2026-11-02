# scripts/

Helper scripts for running the project.

## Files

| File | Owner | Role |
|---|---|---|
| `submit_spark.sh` | **Mariem** | `spark-submit` wrapper — packages the Kafka connector, matches `SPARK_VERSION` |

## Usage

```bash
export SPARK_VERSION=3.5.1
bash scripts/submit_spark.sh
```
