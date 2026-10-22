import json

from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "ecommerce-orders"


def serialize_event(event):
    return json.dumps(
        event,
        ensure_ascii=False,
    ).encode("utf-8")


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=serialize_event,
    )

    # Sengaja tidak memiliki field event_type dan data.
    invalid_event = {
        "event_id": 999999,
        "source": "dlq_test",
    }

    try:
        metadata = producer.send(
            KAFKA_TOPIC,
            value=invalid_event,
        ).get(timeout=30)

        print("Event pengujian terkirim.")
        print(f"Topic     : {metadata.topic}")
        print(f"Partition : {metadata.partition}")
        print(f"Offset    : {metadata.offset}")

    finally:
        producer.close()


if __name__ == "__main__":
    main()
