"""
File        : test_insert.py
Description : Inserts a dummy flow record into Cassandra 'flowguard.alerts'
              and queries it back to verify end-to-end functionality.
              Uses the partition dataset='test' so real data is not mixed in.
"""
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

    # 1. Create sample test row (same columns as spark_consumer.py writes).
    #    Cassandra stores timestamps in milliseconds, so microseconds are dropped
    #    to be able to read the row back with an exact match.
    now = datetime.now(timezone.utc).replace(microsecond=0)

    print("\n[1] Inserting test record...")
    print(f"    Dataset     : test")
    print(f"    Timestamp   : {now}")

    insert_query = """
        INSERT INTO alerts (
            dataset, kafka_timestamp, kafka_partition, kafka_offset,
            event_id, label, mse, prediction, latency_ms
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    session.execute(insert_query, (
        "test",              # dataset (partition key)
        now,                 # kafka_timestamp
        0,                   # kafka_partition
        0,                   # kafka_offset
        "test_0",            # event_id
        "BENIGN",            # real label
        0.085,               # mse
        "ANOMALY",           # prediction
        12.5                 # latency_ms
    ))
    print("    Insert successful!")

    # 2. Query back the inserted record
    print("\n[2] Reading record back from Cassandra...")
    select_query = """
        SELECT dataset, kafka_timestamp, event_id, label, mse, prediction, latency_ms
        FROM alerts
        WHERE dataset = %s AND kafka_timestamp = %s AND kafka_partition = %s AND kafka_offset = %s
    """
    row = session.execute(select_query, ("test", now, 0, 0)).one()

    if row:
        print("\n--- RECORD VERIFIED IN DB ---")
        print(f"Dataset          : {row.dataset}")
        print(f"Kafka Timestamp  : {row.kafka_timestamp}")
        print(f"Event ID         : {row.event_id}")
        print(f"Label            : {row.label}")
        print(f"MSE              : {row.mse}")
        print(f"Prediction       : {row.prediction}")
        print(f"Latency (ms)     : {row.latency_ms}")
        print("-----------------------------\n")
    else:
        print("\n[!] ERROR: Inserted record could not be retrieved.\n")

    cluster.shutdown()

if __name__ == "__main__":
    test_insert_and_read()