"""
File        : test_insert.py
Description : Inserts a dummy flow record into Cassandra 'flowguard.alerts' 
              and queries it back to verify end-to-end functionality.
"""
import uuid
from datetime import datetime, timezone
from cassandra.cluster import Cluster
from cassandra.io.asyncioreactor import AsyncioConnection

CASSANDRA_HOSTS = ['127.0.0.1']
CASSANDRA_PORT = 9042
KEYSPACE = "flowguard"

def test_insert_and_read():
    print("Connecting to Cassandra...")
    cluster = Cluster(
        CASSANDRA_HOSTS,
        port=CASSANDRA_PORT,
        connection_class=AsyncioConnection
    )
    session = cluster.connect(KEYSPACE)

    # 1. Create sample test row
    now = datetime.now(timezone.utc)
    today_bucket = now.strftime('%Y-%m-%d')
    sample_flow_id = uuid.uuid4()

    print(f"\n[1] Inserting test record...")
    print(f"    Date Bucket : {today_bucket}")
    print(f"    Flow ID     : {sample_flow_id}")

    insert_query = """
        INSERT INTO alerts (
            date_bucket, alert_time, flow_id,
            mse_score, threshold, is_anomaly, predicted_label,
            destination_port, flow_duration, total_fwd_packets, total_bwd_packets,
            flow_bytes_s, label
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    session.execute(insert_query, (
        today_bucket,
        now,
        sample_flow_id,
        0.085,               # mse_score
        0.050,               # threshold
        True,                # is_anomaly
        "ANOMALY_EXPLOIT",   # predicted_label
        53,                  # destination_port
        221692,              # flow_duration
        1,                   # total_fwd_packets
        1,                   # total_bwd_packets
        875.088,             # flow_bytes_s
        "BENIGN"             # dataset label
    ))
    print("    Insert successful!")

    # 2. Query back the inserted record
    print("\n[2] Reading record back from Cassandra...")
    select_query = """
        SELECT date_bucket, alert_time, flow_id, mse_score, is_anomaly, predicted_label, destination_port
        FROM alerts
        WHERE date_bucket = %s AND alert_time = %s AND flow_id = %s
    """
    row = session.execute(select_query, (today_bucket, now, sample_flow_id)).one()

    if row:
        print("\n--- RECORD VERIFIED IN DB ---")
        print(f"Date Bucket      : {row.date_bucket}")
        print(f"Alert Time       : {row.alert_time}")
        print(f"Flow ID          : {row.flow_id}")
        print(f"MSE Score        : {row.mse_score}")
        print(f"Is Anomaly       : {row.is_anomaly}")
        print(f"Predicted Label  : {row.predicted_label}")
        print(f"Destination Port : {row.destination_port}")
        print("-----------------------------\n")
    else:
        print("\n[!] ERROR: Inserted record could not be retrieved.\n")

    cluster.shutdown()

if __name__ == "__main__":
    test_insert_and_read()