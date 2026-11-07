# =============================================================================
# File        : cassandra_setup.py
# Module      : streaming
# Owner       : Mariem
# Description : Creates the Cassandra keyspace and tables used by the project
#               with explicit schemas so that Grafana can query them correctly.
#                 - flowguard.all_flows     : every scored flow
#                 - flowguard.alerts        : flows predicted as ANOMALY
#                 - flowguard.batch_metrics : one row per Spark micro-batch
#                   (flows, anomalies, MSE, latency p50/p95/p99, throughput)
#               Partition key = dataset (all_flows, alerts) or pipeline
#               (batch_metrics); rows ordered newest first.
#               Safe to re-run: uses IF NOT EXISTS on all CREATE statements.
#               Run automatically by the cassandra-init service in Docker.
# Input       : env CASSANDRA_HOST, CASSANDRA_PORT, CASSANDRA_KEYSPACE
# Output      : Cassandra keyspace flowguard with tables all_flows, alerts, batch_metrics
# =============================================================================
# Cassandra test code 
# NOT FOR DEPLOYMENT JUST FOR TESTING PURPOSES :)

import os
from cassandra.cluster import Cluster


CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", 9042))
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "flowguard")

def create_cassandra_schema():
    print(f"Connecting to Cassandra at {CASSANDRA_HOST}:{CASSANDRA_PORT}...")
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect()

    # 1. Create Keyspace
    print(f"Creating keyspace '{KEYSPACE}'...")
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{
            'class': 'SimpleStrategy',
            'replication_factor': 1
        }};
    """)

    session.set_keyspace(KEYSPACE)

    # 2. Table: all_flows (Stores every evaluated event)
    print("Creating table 'all_flows'...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS all_flows (
            dataset text,
            kafka_timestamp timestamp,
            kafka_partition int,
            kafka_offset bigint,
            event_id text,
            label text,
            mse double,
            prediction text,
            latency_ms double,
            PRIMARY KEY ((dataset), kafka_timestamp, kafka_partition, kafka_offset)
        ) WITH CLUSTERING ORDER BY (kafka_timestamp DESC, kafka_partition ASC, kafka_offset ASC);
    """)

    # 3. Table: alerts (Stores only records where prediction == 'ANOMALY')
    print("Creating table 'alerts'...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            dataset text,
            kafka_timestamp timestamp,
            kafka_partition int,
            kafka_offset bigint,
            event_id text,
            label text,
            mse double,
            prediction text,
            latency_ms double,
            PRIMARY KEY ((dataset), kafka_timestamp, kafka_partition, kafka_offset)
        ) WITH CLUSTERING ORDER BY (kafka_timestamp DESC, kafka_partition ASC, kafka_offset ASC);
    """)
    
    # 4. Table: batch_metrics (one summary row per Spark micro-batch)
    print("Creating table 'batch_metrics'...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS batch_metrics (
            pipeline text,
            batch_time timestamp,
            batch_id bigint,
            flows int,
            anomalies int,
            anomaly_rate double,
            avg_mse double,
            max_mse double,
            latency_p50_ms double,
            latency_p95_ms double,
            latency_p99_ms double,
            latency_max_ms double,
            processing_s double,
            throughput_eps double,
            PRIMARY KEY ((pipeline), batch_time)
        ) WITH CLUSTERING ORDER BY (batch_time DESC);
    """)


    print("\n[SUCCESS] Cassandra keyspace and tables created successfully!")
    cluster.shutdown()

if __name__ == "__main__":
    create_cassandra_schema()