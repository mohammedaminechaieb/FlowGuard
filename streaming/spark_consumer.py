import os
import sys
import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import joblib
import onnxruntime as ort

from pyspark.sql import SparkSession
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
    os.getenv("MODEL_DIR", str(BASE_DIR / "models"))
)

SCALER_PATH = MODEL_DIR / "scaler.pkl"
ONNX_PATH = MODEL_DIR / "autoencoder.onnx"
FEATURES_PATH = MODEL_DIR / "feature_columns.json"

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

KAFKA_TOPIC = "network-telemetry"

CHECKPOINT_PATH = os.getenv(
    "CHECKPOINT_PATH",
    str(BASE_DIR / "checkpoints" / "spark_inference")
)

THRESHOLD = 0.03222443163394928

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
    StructField("label", StringType(), True)
])


# ============================================================
# 4. INITIALISATION SPARK
# ============================================================

def create_spark_session():

    return (
        SparkSession.builder
        .appName("FlowGuard-NIDS")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
        )
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
        .option("maxOffsetsPerTrigger", "500")
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
            col("event.label").alias("label")
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
    StructField("prediction", StringType(), True)
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

        yield pd.DataFrame({
            "kafka_timestamp": valid_pdf["kafka_timestamp"].values,
            "kafka_partition": valid_pdf["kafka_partition"].values,
            "kafka_offset": valid_pdf["kafka_offset"].values,
            "event_id": valid_pdf["event_id"].values,
            "dataset": valid_pdf["dataset"].values,
            "label": valid_pdf["label"].values,
            "mse": mse_values.astype(float),
            "prediction": predictions
        })


# ============================================================
# 9. BILAN PAR BATCH (diagnostic)
# ============================================================

def log_batch(batch_df, batch_id):

    # persist() évite de relancer le modèle ONNX à chaque action ci-dessous.
    batch_df.persist()

    count = batch_df.count()

    if count:
        print(f"\n[BATCH {batch_id}] {count} événements scorés", flush=True)

        batch_df.groupBy("label", "prediction").count().show(truncate=False)

        batch_df.orderBy("kafka_offset").show(20, truncate=False)

    batch_df.unpersist()

# ============================================================
# 7. EXPORT BATCH VERS CASSANDRA (TOUT LE FLUX + ALERTES)
# ============================================================

def write_to_cassandra(batch_df, batch_id):
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
