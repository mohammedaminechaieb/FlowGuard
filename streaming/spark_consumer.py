import os
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import joblib
import onnxruntime as ort

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    from_json
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    MapType,
    DoubleType,
    TimestampType,
    IntegerType,
    LongType
)


# ============================================================
# 1. CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent

MODEL_DIR = Path(
    os.getenv("MODEL_DIR", str(BASE_DIR / "ai_module" / "artifacts"))
)

SCALER_PATH = MODEL_DIR / "scaler.pkl"
ONNX_PATH = MODEL_DIR / "autoencoder.onnx"
FEATURES_PATH = MODEL_DIR / "feature_columns.json"

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

KAFKA_TOPIC = "network-telemetry"

CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "flowguard")
TABLE_ALL_FLOWS = "all_flows"
TABLE_ALERTS = "alerts"

CHECKPOINT_PATH = os.getenv(
    "CHECKPOINT_PATH",
    str(BASE_DIR / "checkpoints" / "spark_inference")
)

with open(MODEL_DIR / "threshold.json", "r", encoding="utf-8") as file:
    THRESHOLD = float(json.load(file)["threshold"])

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FlowGuard")


# ============================================================
# 2. CHARGEMENT DE LA LISTE DES FEATURES
# ============================================================

if not FEATURES_PATH.exists():
    raise FileNotFoundError(
        f"Fichier des features introuvable : {FEATURES_PATH}"
    )

with open(FEATURES_PATH, "r", encoding="utf-8") as file:
    FEATURE_COLUMNS = json.load(file)

if not isinstance(FEATURE_COLUMNS, list):
    raise ValueError(
        "feature_columns.json doit contenir une liste de noms."
    )

if len(FEATURE_COLUMNS) != 78:
    raise ValueError(
        f"78 features attendues, {len(FEATURE_COLUMNS)} trouvées."
    )

if len(set(FEATURE_COLUMNS)) != 78:
    raise ValueError("La liste contient des features en double.")

# Noms internes sans caractère spécial. Certains noms CICIDS2017 contiennent
# un point ("Fwd Header Length.1") ou un "/" : Spark lirait le point comme
# l'accès à un champ imbriqué. L'ordre des alias suit exactement celui de
# feature_columns.json, donc celui attendu par le scaler et le modèle.
FEATURE_ALIASES = [f"f{i}" for i in range(len(FEATURE_COLUMNS))]

logger.info("Nombre de features : %s", len(FEATURE_COLUMNS))


# ============================================================
# 3. SCHEMA KAFKA
# ============================================================

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("dataset", StringType(), True),
    StructField(
        "features",
        MapType(StringType(), DoubleType()),
        True
    ),
    StructField("label", StringType(), True),
    StructField("send_ts", DoubleType(), True)
])


# ============================================================
# 4. INITIALISATION SPARK
# ============================================================

def create_spark_session():

    return (
        SparkSession.builder
        .appName("FlowGuard-NIDS")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# ============================================================
# 5. LECTURE DE KAFKA
# ============================================================

def read_from_kafka(spark):

    logger.info("Connexion à Kafka : %s", KAFKA_BOOTSTRAP_SERVERS)

    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", os.getenv("MAX_OFFSETS_PER_TRIGGER", "500"))
        .load()
    )


# ============================================================
# 6. PARSING DES EVENEMENTS
# ============================================================

def parse_events(kafka_df):

    parsed_df = (
        kafka_df
        .select(
            col("timestamp").alias("kafka_timestamp"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("value").cast("string").alias("json_value")
        )
        .withColumn(
            "event",
            from_json(col("json_value"), EVENT_SCHEMA)
        )
        .select(
            col("kafka_timestamp"),
            col("kafka_partition"),
            col("kafka_offset"),
            col("event.event_id").alias("event_id"),
            col("event.dataset").alias("dataset"),
            col("event.features").alias("features"),
            col("event.label").alias("label"),
            col("event.send_ts").alias("send_ts")
        )
    )

    return parsed_df


# ============================================================
# 7. PREPARATION DES 78 FEATURES
# ============================================================

def prepare_features(parsed_df):

    # getItem lit la clé du dictionnaire telle quelle (point et "/" compris),
    # puis la colonne est renommée avec un alias sûr (f0 ... f77).
    feature_expressions = [
        col("features").getItem(feature).cast("double").alias(alias)
        for feature, alias in zip(FEATURE_COLUMNS, FEATURE_ALIASES)
    ]

    return parsed_df.select(
        "kafka_timestamp",
        "kafka_partition",
        "kafka_offset",
        "event_id",
        "dataset",
        "label",
        "send_ts",
        *feature_expressions
    )


# ============================================================
# 8. INFERENCE ONNX ET CALCUL DU MSE
# ============================================================

OUTPUT_SCHEMA = StructType([
    StructField("kafka_timestamp", TimestampType(), True),
    StructField("kafka_partition", IntegerType(), True),
    StructField("kafka_offset", LongType(), True),
    StructField("event_id", StringType(), True),
    StructField("dataset", StringType(), True),
    StructField("label", StringType(), True),
    StructField("mse", DoubleType(), True),
    StructField("prediction", StringType(), True),
    StructField("latency_ms", DoubleType(), True),
    StructField("scored_at", TimestampType(), True)
])


def worker_log(message):
    # Les logs Python des processus de travail ne passent pas par le logging
    # configuré dans le pilote : on écrit directement sur stderr pour que le
    # message apparaisse dans `docker logs`.
    print(message, file=sys.stderr, flush=True)


def run_inference(iterator):

    scaler = joblib.load(str(SCALER_PATH))
    session = ort.InferenceSession(
        str(ONNX_PATH),
        providers=["CPUExecutionProvider"]
    )
    input_name = session.get_inputs()[0].name
    output_name = session.get_outputs()[0].name

    for pdf in iterator:
        if pdf.empty:
            continue

        X = pdf[FEATURE_ALIASES].to_numpy(dtype=np.float32, copy=True)
        valid_mask = np.isfinite(X).all(axis=1)

        dropped = int((~valid_mask).sum())
        if dropped:
            worker_log(
                f"[INFERENCE] {dropped} événement(s) écarté(s) : valeur "
                f"manquante ou infinie dans les features."
            )

        if not valid_mask.any():
            continue

        valid_pdf = pdf.loc[valid_mask].copy()
        X = X[valid_mask]

        X_scaled = scaler.transform(X).astype(np.float32)

        reconstructed = session.run([output_name], {input_name: X_scaled})[0]
        reconstructed = np.asarray(reconstructed, dtype=np.float32)

        mse_values = np.mean(np.square(X_scaled - reconstructed), axis=1)
        predictions = np.where(mse_values > THRESHOLD, "ANOMALY", "BENIGN")

        worker_log(f"[INFERENCE] {len(predictions)} événements scorés par l'autoencodeur.")
        
        # Latence producer -> score (ms). Ancien message sans send_ts -> null.
        send_ts = pd.to_numeric(valid_pdf["send_ts"], errors="coerce").to_numpy(dtype=float)
        latency_ms = pd.array((time.time() - send_ts) * 1000.0, dtype="Float64")
        scored_at = pd.Timestamp.now(tz="UTC")  # moment où Spark a scoré le batch

        yield pd.DataFrame({
            "kafka_timestamp": valid_pdf["kafka_timestamp"].values,
            "kafka_partition": valid_pdf["kafka_partition"].values,
            "kafka_offset": valid_pdf["kafka_offset"].values,
            "event_id": valid_pdf["event_id"].values,
            "dataset": valid_pdf["dataset"].values,
            "label": valid_pdf["label"].values,
            "mse": mse_values.astype(float),
            "prediction": predictions,
            "latency_ms": latency_ms,
            "scored_at": [scored_at] * len(predictions)
        }) 

# ============================================================
# 9. EXPORT BATCH VERS CASSANDRA (TOUT LE FLUX + ALERTES)
# ============================================================
TABLE_BATCH_METRICS = "batch_metrics"

BATCH_METRICS_SCHEMA = StructType([
    StructField("pipeline", StringType(), False),
    StructField("batch_time", TimestampType(), False),
    StructField("batch_id", LongType(), True),
    StructField("flows", IntegerType(), True),
    StructField("anomalies", IntegerType(), True),
    StructField("anomaly_rate", DoubleType(), True),
    StructField("avg_mse", DoubleType(), True),
    StructField("max_mse", DoubleType(), True),
    StructField("latency_p50_ms", DoubleType(), True),
    StructField("latency_p95_ms", DoubleType(), True),
    StructField("latency_p99_ms", DoubleType(), True),
    StructField("latency_max_ms", DoubleType(), True),
    StructField("processing_s", DoubleType(), True),
    StructField("throughput_eps", DoubleType(), True)
])


def write_batch_metrics(batch_df, batch_id, count, alerts_count, processing_s):
    """Une ligne de synthèse par batch -> table batch_metrics (Grafana + latency_report.py)."""
    stats = batch_df.agg(
        F.avg("mse").alias("avg_mse"),
        F.max("mse").alias("max_mse"),
        F.percentile_approx("latency_ms", [0.5, 0.95, 0.99], 10000).alias("lat"),
        F.max("latency_ms").alias("lat_max")
    ).first()

    p50, p95, p99 = stats["lat"] if stats["lat"] else (None, None, None)

    def as_float(value):
        return None if value is None else float(value)

    row = (
        "spark",
        datetime.now(timezone.utc),
        int(batch_id),
        int(count),
        int(alerts_count),
        float(alerts_count) / count,
        as_float(stats["avg_mse"]),
        as_float(stats["max_mse"]),
        as_float(p50),
        as_float(p95),
        as_float(p99),
        as_float(stats["lat_max"]),
        float(processing_s),
        float(count) / processing_s if processing_s > 0 else None
    )
    metrics_df = batch_df.sparkSession.createDataFrame([row], schema=BATCH_METRICS_SCHEMA)

    metrics_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", CASSANDRA_KEYSPACE) \
        .option("table", TABLE_BATCH_METRICS) \
        .mode("append") \
        .save()

    p95_text = "n/a" if p95 is None else f"{p95:.0f} ms"
    print(f"[METRIQUES] latence p95 = {p95_text} | débit = {row[-1]:.0f} évts/s "
          f"| traitement = {processing_s:.2f} s", flush=True)

def write_to_cassandra(batch_df, batch_id):
    started = time.time()
    batch_df.persist()
    count = batch_df.count()

    if count > 0:
        print(f"\n[BATCH {batch_id}] {count} événements traités.", flush=True)

        # 1. Écriture de l'intégralité du flux dans la table `all_flows`
        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", CASSANDRA_KEYSPACE) \
            .option("table", TABLE_ALL_FLOWS) \
            .mode("append") \
            .save()
        print(f"[CASSANDRA] {count} flux enregistrés dans {CASSANDRA_KEYSPACE}.{TABLE_ALL_FLOWS}", flush=True)

        # 2. Écriture filtrée des alertes dans la table `alerts`
        alerts_df = batch_df.filter(col("prediction") == "ANOMALY")
        alerts_count = alerts_df.count()

        if alerts_count > 0:
            alerts_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .option("keyspace", CASSANDRA_KEYSPACE) \
                .option("table", TABLE_ALERTS) \
                .mode("append") \
                .save()
            print(f"[ALERTES] {alerts_count} anomalies enregistrées dans {CASSANDRA_KEYSPACE}.{TABLE_ALERTS}", flush=True)
            
        # 3. Ligne de synthèse du batch (latence, débit) -> batch_metrics 
        write_batch_metrics(batch_df, batch_id, count, alerts_count, time.time() - started)

    batch_df.unpersist()


# ============================================================
# 8. MAIN STREAMING PIPELINE
# ============================================================

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("FlowGuard - Démarrage de l'inférence et de l'export Cassandra...")

    kafka_df = read_from_kafka(spark)
    parsed_df = parse_events(kafka_df)
    features_df = prepare_features(parsed_df)

    predictions_df = features_df.mapInPandas(
        run_inference,
        schema=OUTPUT_SCHEMA
    )

    query = (
        predictions_df
        .writeStream
        .foreachBatch(write_to_cassandra)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="5 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
