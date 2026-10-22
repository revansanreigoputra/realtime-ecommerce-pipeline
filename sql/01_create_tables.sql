CREATE TABLE IF NOT EXISTS raw_events (
    id BIGSERIAL PRIMARY KEY,
    event_fingerprint CHAR(64) NOT NULL UNIQUE,
    event_id BIGINT NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    source VARCHAR(100) NOT NULL,
    kafka_topic VARCHAR(200) NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    payload JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_raw_events_kafka_position
        UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS idx_raw_events_received_at
    ON raw_events (received_at);

CREATE INDEX IF NOT EXISTS idx_raw_events_payload
    ON raw_events USING GIN (payload);
