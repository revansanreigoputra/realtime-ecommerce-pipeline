import json
import logging
import os
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PATH = PROJECT_ROOT / "data" / "raw" / "online_retail_II.xlsx"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "ecommerce-orders"

# Default hanya 10 event untuk pengujian awal.
MAX_EVENTS = int(os.getenv("MAX_EVENTS", "10"))
EVENT_DELAY_SECONDS = 0.2


def serialize_event(event):
    """Mengubah event Python menjadi JSON bytes."""
    return json.dumps(
        event,
        default=str,
        ensure_ascii=False,
    ).encode("utf-8")


def main():
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Dataset tidak ditemukan: {DATA_PATH}")

    if MAX_EVENTS < 1:
        raise ValueError("MAX_EVENTS harus minimal 1.")

    logging.info("Membaca dataset: %s", DATA_PATH)

    # Ambil sejumlah baris saja agar pengujian tidak mengirim semua data.
    df = pd.read_excel(
        DATA_PATH,
        sheet_name="Year 2009-2010",
        nrows=MAX_EVENTS,
    )

    # Nama kolom dibuat konsisten untuk event JSON.
    df.columns = [
        str(column).strip().replace(" ", "_")
        for column in df.columns
    ]

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=serialize_event,
        acks="all",
        retries=5,
    )

    sent = 0

    try:
        for row in df.to_dict(orient="records"):
            event = {
                "event_id": sent + 1,
                "event_type": "retail_transaction",
                "source": "online_retail_ii",
                "data": row,
            }

            producer.send(KAFKA_TOPIC, value=event).get(timeout=30)
            sent += 1

            logging.info(
                "Event %s terkirim | Invoice=%s",
                sent,
                row.get("Invoice"),
            )

            time.sleep(EVENT_DELAY_SECONDS)

        producer.flush()
    finally:
        producer.close()

    logging.info("Selesai. Total event terkirim: %s", sent)


if __name__ == "__main__":
    main()
