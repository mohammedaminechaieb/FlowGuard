# app/core/es_client.py
# Elasticsearch access for the pages: one connection, plus query helpers returning pandas DataFrames:
# latest alerts, alerts over time (date histogram), top source IPs (terms aggregation),
# batch metrics, and a health check. All queries accept the sidebar filters.
