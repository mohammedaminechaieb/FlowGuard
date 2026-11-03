# app/pages/1_Dashboard.py
# Monitoring dashboard, read from Elasticsearch (flowguard-alerts and flowguard-metrics).
# KPI cards: events scored, events flagged, flag rate, p95 latency.
# Charts: alerts over time, top source IPs, alerts per attack type (when labels are known),
# latency per micro-batch. Table of the latest alerts with their top features.
# Sidebar filters: time range, source IP, minimum reconstruction error.
