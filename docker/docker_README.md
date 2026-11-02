# docker/

Infrastructure: Kafka (KRaft), Elasticsearch 8.15, Kibana 8.15.

## Files

| File | Owner | Role |
|---|---|---|
| `docker-compose.yml` | **Amine** | Defines Kafka, Elasticsearch and Kibana services and the shared `flowguard-net` network |
| `kibana/flowguard_dashboard.ndjson` | **Mariem** | Exported Kibana dashboard — import via Kibana → Stack Management → Saved Objects |
| `kibana/.gitkeep` | — | Keeps the folder in git before the dashboard is exported |

## Services

| Service | Image | Port |
|---|---|---|
| `kafka` | `apache/kafka:3.8.0` (KRaft, no ZooKeeper) | `9092` (host), `29092` (inter-container) |
| `elasticsearch` | `docker.elastic.co/elasticsearch/elasticsearch:8.15.3` | `9200` |
| `kibana` | `docker.elastic.co/kibana/kibana:8.15.3` | `5601` |

## Quick start

```bash
docker compose -f docker/docker-compose.yml up -d
```

Open Kibana at http://localhost:5601 — wait ~30 s for it to be ready.

## Notes

- `xpack.security.enabled=false` — demo only.
- Elasticsearch data is persisted in the `esdata` Docker volume.
- If Spark runs in a container, attach it to `flowguard-net` and use `kafka:29092`.
