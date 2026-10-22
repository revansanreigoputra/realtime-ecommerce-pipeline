import json
import time
import uuid

import pytest
from kafka import KafkaProducer

from pipeline.consumer import fingerprint_event
from pipeline.db import get_connection


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "ecommerce-orders"


def make_valid_event():
    """Membuat event transaksi valid dengan identitas unik."""
    return {
        "event_id": int(time.time() * 1000),
        "event_type": "retail_transaction",
        "source": "integration_test",
        "data": {
            "Invoice": f"TEST-{uuid.uuid4().hex[:8]}",
            "StockCode": "TEST01",
            "Quantity": 2,
            "InvoiceDate": "2009-12-01 07:45:00",
            "Price": 10.0,
            "Country": "United Kingdom",
        },
    }


def send_event(event):
    """Mengirim event ke Kafka dan mengembalikan metadata pesan."""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(
            value,
            ensure_ascii=False,
        ).encode("utf-8"),
    )

    try:
        return producer.send(
            KAFKA_TOPIC,
            value=event,
        ).get(timeout=30)
    finally:
        producer.close()


def wait_for_database_record(query, params, timeout=30):
    """Memeriksa database berulang kali sampai record ditemukan."""
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        conn = get_connection()

        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                row = cur.fetchone()
        finally:
            conn.close()

        if row is not None:
            return row

        time.sleep(0.5)

    return None


@pytest.mark.integration
def test_valid_kafka_event_is_stored_in_postgres():
    event = make_valid_event()
    metadata = send_event(event)

    fingerprint = fingerprint_event(event)

    row = wait_for_database_record(
        """
        SELECT event_id, payload, kafka_topic,
               kafka_partition, kafka_offset
        FROM raw_events
        WHERE event_fingerprint = %s
        """,
        (fingerprint,),
    )

    assert row is not None, (
        "Event valid tidak ditemukan di raw_events. "
        f"Topic={metadata.topic}, "
        f"partition={metadata.partition}, "
        f"offset={metadata.offset}"
    )

    assert row[0] == event["event_id"]
    assert row[1] == event
    assert row[2] == metadata.topic
    assert row[3] == metadata.partition
    assert row[4] == metadata.offset


@pytest.mark.integration
def test_invalid_kafka_event_is_stored_in_dlq():
    # Event sengaja tidak memiliki event_type dan data.
    event = {
        "event_id": int(time.time() * 1000),
        "source": "integration_test_dlq",
        "test_id": str(uuid.uuid4()),
    }

    metadata = send_event(event)

    row = wait_for_database_record(
        """
        SELECT event_id, error_reason, payload,
               kafka_topic, kafka_partition, kafka_offset
        FROM dead_letter_events
        WHERE kafka_topic = %s
          AND kafka_partition = %s
          AND kafka_offset = %s
        """,
        (
            metadata.topic,
            metadata.partition,
            metadata.offset,
        ),
    )

    assert row is not None, (
        "Event tidak valid tidak ditemukan di DLQ. "
        f"Topic={metadata.topic}, "
        f"partition={metadata.partition}, "
        f"offset={metadata.offset}"
    )

    assert row[0] == event["event_id"]
    assert "event_type" in row[1]
    assert "data" in row[1]
    assert row[2] == event
    assert row[3] == metadata.topic
    assert row[4] == metadata.partition
    assert row[5] == metadata.offset

@pytest.mark.integration
def test_duplicate_event_is_stored_only_once():
    event = make_valid_event()
    fingerprint = fingerprint_event(event)

    # Kirim event identik dua kali ke Kafka.
    first_metadata = send_event(event)
    second_metadata = send_event(event)

    # Pastikan kedua pesan benar-benar mendapat posisi berbeda.
    assert (
        first_metadata.topic,
        first_metadata.partition,
        first_metadata.offset,
    ) != (
        second_metadata.topic,
        second_metadata.partition,
        second_metadata.offset,
    )

    # Tunggu sampai consumer memproses setidaknya satu salinan.
    row = wait_for_database_record(
        """
        SELECT COUNT(*)
        FROM raw_events
        WHERE event_fingerprint = %s
        """,
        (fingerprint,),
    )

    assert row is not None

    # Tunggu agar kesempatan pemrosesan kedua juga tersedia.
    deadline = time.monotonic() + 5

    while time.monotonic() < deadline:
        conn = get_connection()

        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM raw_events
                    WHERE event_fingerprint = %s
                    """,
                    (fingerprint,),
                )
                count = cur.fetchone()[0]
        finally:
            conn.close()

        if count > 1:
            break

        time.sleep(0.25)

    assert count == 1, (
        "Event duplikat tersimpan lebih dari satu kali: "
        f"fingerprint={fingerprint}, jumlah={count}"
    )