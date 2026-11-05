# docker/

Infrastructure: Kafka (KRaft), Cassandra 5.0, Grafana 10.4.

## Files

| File | Owner | Role |
|---|---|---|
| `docker-compose.yml` | **Amine** | Defines Kafka, Cassandra and Grafana services and the shared `flowguard-net` network |
| `grafana/flowguard_dashboard.json` | **Mariem** | Exported Grafana dashboard — import via Grafana → Dashboards → Import |
| `grafana/.gitkeep` | — | Keeps the folder in git before the dashboard is exported |

## Services

| Service | Image | Port |
|---|---|---|
| `kafka` | `apache/kafka:3.8.0` (KRaft, no ZooKeeper) | `9092` (host), `29092` (inter-container) |
| `cassandra` | `cassandra:5.0` | `9042` |
| `grafana` | `grafana/grafana:10.4.0` | `3000` |

## Quick start

```bash
docker compose -f docker/docker-compose.yml up -d
```

Wait ~30 s for Cassandra to be ready, then create the schema:

```bash
python streaming/cassandra_setup.py
```

Open Grafana at http://localhost:3000 (admin / admin).
Install the Cassandra plugin: Grafana → Plugins → search `cassandra` → install `hadesarchitect-cassandra-datasource`.
Then import `grafana/flowguard_dashboard.json` via Grafana → Dashboards → Import.

## Notes

- `GF_SECURITY_ADMIN_PASSWORD=admin` — demo only, change for any real deployment.
- Cassandra data is persisted in the `cassandra_data` Docker volume.
- If Spark runs in a container, attach it to `flowguard-net` and use `cassandra:9042` and `kafka:29092`.
- Wait for Cassandra's healthcheck to pass before running `cassandra_setup.py`.