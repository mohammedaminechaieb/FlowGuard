# =============================================================================
# File        : es_setup.py
# Module      : streaming
# Owner       : Eya
# Description : Creates the two Elasticsearch indices used by the project with
#               explicit field mappings so that Kibana can plot them correctly.
#                 - flowguard-alerts  : one document per flagged (anomalous) flow
#                 - flowguard-metrics : one document per Spark micro-batch
#                   (batch_id, record count, mean MSE, p50/p95/p99 latency)
#               Safe to re-run: skips creation if the index already exists.
# Input       : .env  (ES_URL, ES_ALERTS_INDEX, ES_METRICS_INDEX)
# Output      : Elasticsearch indices  flowguard-alerts  and  flowguard-metrics
# =============================================================================
# streaming/es_setup.py
# Create the Elasticsearch indices and mappings: flowguard-alerts (flagged flows) and flowguard-metrics (one document per micro-batch).
