import argparse
import json
import math
import time
from pathlib import Path

import pyarrow.parquet as pq
from kafka import KafkaProducer


# --------------------------------------------------
# Configuration
# --------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent
PARQUET_DIR = BASE_DIR / "data" / "parquet"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "network-telemetry"

DATASETS = {
    "train": "train.parquet",
    "validation": "validation.parquet",
    "tuesday": "test_tuesday.parquet",
    "wednesday": "test_wednesday.parquet",
    "thursday": "test_thursday.parquet",
    "friday": "test_friday.parquet",
}


def clean_value(value):
    """Convertit les valeurs non compatibles JSON en valeurs sûres."""

    if value is None:
        return None

    if isinstance(value, float) and not math.isfinite(value):
        return None

    return value


def create_producer():
    """Crée le producteur Kafka."""

    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        acks="all",
        retries=5,
        compression_type="gzip",
        linger_ms=20,
        batch_size=32768,
        request_timeout_ms=30000,
        max_block_ms=30000,
    )


def stream_parquet(
    producer,
    dataset_name,
    filename,
    batch_size,
    delay,
    max_events,
    stats,
):
    """
    Envoie un fichier Parquet vers Kafka de façon asynchrone.

    `stats` est un dictionnaire partagé avec main() :
      - sent      : événements remis au producteur
      - confirmed : événements confirmés par Kafka (callback)
      - failed    : événements en échec (errback)
    """

    file_path = PARQUET_DIR / filename

    if not file_path.exists():
        raise FileNotFoundError(f"Fichier Parquet introuvable : {file_path}")

    print(f"\nDataset : {dataset_name}")
    print(f"Fichier : {file_path}")

    parquet_file = pq.ParquetFile(file_path)
    all_columns = parquet_file.schema_arrow.names

    # Label est conservé comme métadonnée, pas comme feature.
    feature_columns = [name for name in all_columns if name != "Label"]
    columns_to_read = feature_columns + (["Label"] if "Label" in all_columns else [])

    print(f"Nombre de features envoyées : {len(feature_columns)}")
    print(f"Topic Kafka : {KAFKA_TOPIC}")

    def on_success(_metadata):
        stats["confirmed"] += 1

    def on_error(exc):
        stats["failed"] += 1
        print(f"[ERREUR] Envoi Kafka échoué : {exc}")

    row_index = 0

    for batch in parquet_file.iter_batches(
        batch_size=batch_size,
        columns=columns_to_read,
    ):
        for record in batch.to_pylist():

            if max_events is not None and stats["sent"] >= max_events:
                producer.flush(timeout=30)
                return

            features = {
                column: clean_value(record.get(column))
                for column in feature_columns
            }

            event_id = f"{dataset_name}_{row_index}"

            event = {
                "event_id": event_id,
                "dataset": dataset_name,
                "send_ts": time.time(),
                "features": features,
                "label": record.get("Label"),
            }

            key_bytes = event_id.encode("utf-8")
            value_bytes = json.dumps(
                event,
                allow_nan=False,
                ensure_ascii=False,
            ).encode("utf-8")

            # Envoi asynchrone : le batching Kafka (linger_ms, batch_size,
            # gzip) reste actif. La confirmation arrive via les callbacks.
            producer.send(
                KAFKA_TOPIC,
                key=key_bytes,
                value=value_bytes,
            ).add_callback(on_success).add_errback(on_error)

            row_index += 1
            stats["sent"] += 1

            if stats["sent"] % 1000 == 0:
                print(
                    f"[KAFKA] {stats['sent']} envoyés, "
                    f"{stats['confirmed']} confirmés"
                )

            if delay > 0:
                time.sleep(delay)

    producer.flush(timeout=30)

    print(f"[TERMINE] {dataset_name} : {row_index} événements envoyés")


def main():
    parser = argparse.ArgumentParser(
        description="Envoi des données CICIDS2017 Parquet vers Kafka"
    )

    parser.add_argument(
        "--dataset",
        choices=list(DATASETS.keys()) + ["test", "all"],
        default="wednesday",
        help="Dataset à envoyer",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Taille des lots de lecture du fichier Parquet",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0,
        help="Délai entre deux événements, en secondes",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Nombre maximal d'événements à envoyer",
    )

    args = parser.parse_args()

    if args.batch_size <= 0:
        parser.error("--batch-size doit être supérieur à 0")

    if args.delay < 0:
        parser.error("--delay ne peut pas être négatif")

    if args.max_events is not None and args.max_events <= 0:
        parser.error("--max-events doit être supérieur à 0")

    if args.dataset == "all":
        selected_datasets = list(DATASETS.items())
    elif args.dataset == "test":
        selected_datasets = [
            (name, filename)
            for name, filename in DATASETS.items()
            if name not in ("train", "validation")
        ]
    else:
        selected_datasets = [(args.dataset, DATASETS[args.dataset])]

    producer = None
    stats = {"sent": 0, "confirmed": 0, "failed": 0}

    try:
        print(f"Connexion à Kafka : {KAFKA_BOOTSTRAP_SERVERS}")
        producer = create_producer()

        for dataset_name, filename in selected_datasets:
            stream_parquet(
                producer=producer,
                dataset_name=dataset_name,
                filename=filename,
                batch_size=args.batch_size,
                delay=args.delay,
                max_events=args.max_events,
                stats=stats,
            )

            if args.max_events is not None and stats["sent"] >= args.max_events:
                break

    except KeyboardInterrupt:
        print("\n[INFO] Arrêt demandé.")

    except Exception as exc:
        print(f"\n[ERREUR] {type(exc).__name__}: {exc}")
        raise

    finally:
        # flush() attend les confirmations restantes : les compteurs
        # affichés ensuite sont donc corrects, même après Ctrl+C.
        if producer is not None:
            producer.flush(timeout=30)
            producer.close(timeout=30)

        print(
            f"\nEnvoyés : {stats['sent']} | "
            f"confirmés par Kafka : {stats['confirmed']} | "
            f"échecs : {stats['failed']}"
        )


if __name__ == "__main__":
    main()