# docker/

Infrastructure: Kafka (KRaft), Cassandra 5.0, Spark 3.5 (Python 3.11, Java 17), Grafana 10.4.

## Files

| File | Owner | Role |
|---|---|---|
| `docker-compose.yml` | **Amine & Eya** | All services and the shared `flowguard-net` network |
| `spark.Dockerfile` | **Amine & Eya** | Spark image: Python 3.11, Java 17, PySpark 3.5.3 and the model libraries (versions match the artifacts) |
| `Grafana/provisioning/` | **Amine** | Automatic setup at Grafana start: Cassandra data source and dashboard provider |
| `Grafana/dashboards/flowguard_dashboard.json` | **Amine & Mariem** | FlowGuard dashboard as code, loaded automatically |

## Services

| Service | Image | Port | Role |
|---|---|---|---|
| `kafka` | `apache/kafka:3.8.0` (KRaft) | `9092` (host), `29092` (containers) | Message broker |
| `cassandra` | `cassandra:5.0` | `9042` | Flows, alerts and batch metrics |
| `cassandra-init` | `flowguard-spark` | — | One-shot: creates/updates the tables, then exits |
| `spark` | `flowguard-spark` | `4040` (Spark UI) | Inference job `streaming/spark_consumer.py` |
| `spark-archive` | `flowguard-spark` | `4041` | Optional (profile `archive`): raw stream → HDFS |
| `grafana` | `grafana/grafana:10.4.2` | `3000` | Dashboard |

## Quick start

```bash
docker compose -f docker/docker-compose.yml up -d
docker compose -f docker/docker-compose.yml ps -a     # cassandra-init should be "Exited (0)"
```

Open Grafana at http://localhost:3000 (admin / admin) → Dashboards → FlowGuard.
The Cassandra plugin, data source and dashboard are installed automatically.

HDFS archive (needs the class Hadoop containers running with HDFS started):

```bash
docker compose -f docker/docker-compose.yml --profile archive up -d spark-archive
```

## Notes

- `GF_SECURITY_ADMIN_PASSWORD=admin` — demo only.
- Cassandra data is persisted in the `cassandra_data` Docker volume.
- After a table change, reset once with `docker exec cassandra cqlsh -e "DROP KEYSPACE IF EXISTS flowguard;"`, then `up -d` again.
- Model files are mounted read-only from `ai_module/artifacts/`.