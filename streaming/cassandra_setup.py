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

"""
File        : streaming/cassandra_setup.py
Module      : FlowGuard Cassandra Storage Layer
Description : Initializes keyspace and wide-schema 'alerts' table (79 features + alert metadata)
              along with 'flow_metrics' summary table using AsyncioConnection for Python 3.12+.
"""
import sys
from cassandra.cluster import Cluster
from cassandra.io.asyncioreactor import AsyncioConnection

CASSANDRA_HOSTS = ['127.0.0.1']
CASSANDRA_PORT = 9042
KEYSPACE = "flowguard"

def setup_cassandra_schema():
    print(f"Connecting to Cassandra at {CASSANDRA_HOSTS}:{CASSANDRA_PORT} via AsyncioConnection...")
    
    try:
        cluster = Cluster(
            CASSANDRA_HOSTS,
            port=CASSANDRA_PORT,
            connection_class=AsyncioConnection
        )
        session = cluster.connect()
        print("Connected successfully!")
    except Exception as e:
        print(f"Failed to connect to Cassandra: {e}")
        sys.exit(1)

    # 1. Create Keyspace
    print(f"Creating keyspace '{KEYSPACE}'...")
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
    """)
    session.set_keyspace(KEYSPACE)

    # 2. Wide Table: Security Alerts (79 Features + Detection Metadata)
    print("Creating wide table 'alerts'...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            -- Partition & Primary Metadata
            date_bucket text,
            alert_time timestamp,
            flow_id uuid,

            -- Anomaly Detection Metadata
            mse_score double,
            threshold double,
            is_anomaly boolean,
            predicted_label text,

            -- Full CICIDS2017 Dataset Features (79 Columns)
            destination_port int,
            flow_duration bigint,
            total_fwd_packets bigint,
            total_bwd_packets bigint,
            total_len_fwd_packets double,
            total_len_bwd_packets double,
            fwd_pkt_len_max double,
            fwd_pkt_len_min double,
            fwd_pkt_len_mean double,
            fwd_pkt_len_std double,
            bwd_pkt_len_max double,
            bwd_pkt_len_min double,
            bwd_pkt_len_mean double,
            bwd_pkt_len_std double,
            flow_bytes_s double,
            flow_packets_s double,
            flow_iat_mean double,
            flow_iat_std double,
            flow_iat_max double,
            flow_iat_min double,
            fwd_iat_total double,
            fwd_iat_mean double,
            fwd_iat_std double,
            fwd_iat_max double,
            fwd_iat_min double,
            bwd_iat_total double,
            bwd_iat_mean double,
            bwd_iat_std double,
            bwd_iat_max double,
            bwd_iat_min double,
            fwd_psh_flags int,
            bwd_psh_flags int,
            fwd_urg_flags int,
            bwd_urg_flags int,
            fwd_header_len bigint,
            bwd_header_len bigint,
            fwd_packets_s double,
            bwd_packets_s double,
            min_pkt_len double,
            max_pkt_len double,
            pkt_len_mean double,
            pkt_len_std double,
            pkt_len_var double,
            fin_flag_cnt int,
            syn_flag_cnt int,
            rst_flag_cnt int,
            psh_flag_cnt int,
            ack_flag_cnt int,
            urg_flag_cnt int,
            cwe_flag_cnt int,
            ece_flag_cnt int,
            down_up_ratio double,
            avg_pkt_size double,
            avg_fwd_seg_size double,
            avg_bwd_seg_size double,
            fwd_header_len_1 bigint,
            fwd_avg_bytes_bulk double,
            fwd_avg_packets_bulk double,
            fwd_avg_bulk_rate double,
            bwd_avg_bytes_bulk double,
            bwd_avg_packets_bulk double,
            bwd_avg_bulk_rate double,
            subflow_fwd_pkts bigint,
            subflow_fwd_bytes bigint,
            subflow_bwd_pkts bigint,
            subflow_bwd_bytes bigint,
            init_win_bytes_fwd int,
            init_win_bytes_bwd int,
            act_data_pkt_fwd int,
            min_seg_size_fwd int,
            active_mean double,
            active_std double,
            active_max double,
            active_min double,
            idle_mean double,
            idle_std double,
            idle_max double,
            idle_min double,
            label text,

            PRIMARY KEY ((date_bucket), alert_time, flow_id)
        ) WITH CLUSTERING ORDER BY (alert_time DESC);
    """)

    # 3. Table: Flow Metrics (Summary aggregations for Grafana dashboards)
    print("Creating summary table 'flow_metrics'...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS flow_metrics (
            date_bucket text,
            window_start timestamp,
            total_flows bigint,
            anomaly_count bigint,
            avg_mse double,
            max_mse double,
            PRIMARY KEY ((date_bucket), window_start)
        ) WITH CLUSTERING ORDER BY (window_start DESC);
    """)

    print("\nFlowGuard Cassandra schema initialized successfully!")
    cluster.shutdown()

if __name__ == "__main__":
    setup_cassandra_schema()