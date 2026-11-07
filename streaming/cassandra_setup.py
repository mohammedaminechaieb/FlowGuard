# =============================================================================
# File        : cassandra_setup.py
# Module      : streaming
# Owner       : Mariem
# Description : Creates the Cassandra keyspace and tables used by the project
#               with explicit schemas so that Grafana can query them correctly.
#                 - flowguard.alerts  : one row per flagged (anomalous) flow
#                 - flowguard.metrics : one row per Spark micro-batch
#                   (batch_id, record count, mean MSE, p50/p95/p99 latency)
#               Partition key includes a time bucket to avoid unbounded
#               partitions for a single attacking host.
#               Safe to re-run: uses IF NOT EXISTS on all CREATE statements.
# Input       : .env  (CASSANDRA_HOST, CASSANDRA_KEYSPACE)
# Output      : Cassandra keyspace flowguard with tables alerts and metrics
# =============================================================================
# Cassandra test code 
# NOT FOR DEPLOYMENT JUST FOR TESTING PURPOSES :)

import os
import time
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
            PRIMARY KEY ((dataset), kafka_timestamp, kafka_partition, kafka_offset)
        ) WITH CLUSTERING ORDER BY (kafka_timestamp DESC, kafka_partition ASC, kafka_offset ASC);
    """)

    print("\n[SUCCESS] Cassandra keyspace and tables created successfully!")
    cluster.shutdown()

if __name__ == "__main__":
    create_cassandra_schema()