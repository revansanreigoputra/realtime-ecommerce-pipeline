
import hashlib
import json
import logging

from kafka import KafkaConsumer
from psycopg.types.json import Jsonb

from pipeline.db import get_connection
from pipeline.validation import validate_event

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "ecommerce-orders"
CONSUMER_GROUP = "ecommerce-postgres-writer"


def fingerprint_event(event):
    """Membuat SHA-256 dari event yang dinormalisasi."""
    serialized = json.dumps(
        event,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def main():
    conn = get_connection()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
    )

    logging.info("Consumer aktif; menunggu pesan Kafka")

    try:
        for message in consumer:
            event = message.value

            valid, reason = validate_event(event)

            try:
                with conn.cursor() as cur:
                    if valid:
                        fingerprint = fingerprint_event(event)

                        cur.execute(
                            """
                            INSERT INTO raw_events (
                                event_fingerprint,
                                event_id,
                                event_type,
                                source,
                                kafka_topic,
                                kafka_partition,
                                kafka_offset,
                                payload
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT DO NOTHING
                            """,
                            (
                                fingerprint,
                                event["event_id"],
                                event["event_type"],
                                event["source"],
                                message.topic,
                                message.partition,
                                message.offset,
                                Jsonb(event),
                            ),
                        )

                        logging.info(
                            "Event valid | event_id=%s | partition=%s | offset=%s",
                            event["event_id"],
                            message.partition,
                            message.offset,
                        )

                    else:
                        event_id = (
                            event.get("event_id")
                            if isinstance(event, dict)
                            else None
                        )
                        event_type = (
                            event.get("event_type")
                            if isinstance(event, dict)
                            else None
                        )
                        source = (
                            event.get("source")
                            if isinstance(event, dict)
                            else None
                        )

                        cur.execute(
                            """
                            INSERT INTO dead_letter_events (
                                event_id,
                                event_type,
                                source,
                                kafka_topic,
                                kafka_partition,
                                kafka_offset,
                                error_reason,
                                payload
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT DO NOTHING
                            """,
                            (
                                event_id,
                                event_type,
                                source,
                                message.topic,
                                message.partition,
                                message.offset,
                                reason,
                                Jsonb(event) if isinstance(event, dict) else None,
                            ),
                        )

                        logging.warning(
                            "Event masuk DLQ | partition=%s | offset=%s | alasan=%s",
                            message.partition,
                            message.offset,
                            reason,
                        )

                # Simpan hasil insert sebelum mengakui offset Kafka.
                conn.commit()

                # Commit manual setelah penyimpanan database berhasil.
                consumer.commit()

            except Exception:
                conn.rollback()
                logging.exception(
                    "Gagal memproses event | partition=%s | offset=%s",
                    message.partition,
                    message.offset,
                )
                raise

    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
