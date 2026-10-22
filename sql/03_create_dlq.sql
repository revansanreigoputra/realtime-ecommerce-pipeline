CREATE TABLE IF NOT EXISTS dead_letter_events (
    id BIGSERIAL PRIMARY KEY,

    event_id BIGINT,
    event_type VARCHAR(100),
    source VARCHAR(100),

    kafka_topic VARCHAR(200) NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,

    error_reason TEXT NOT NULL,
    payload JSONB,

    failed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    retry_count INTEGER NOT NULL DEFAULT 0,

    CONSTRAINT uq_dead_letter_kafka_position
        UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS idx_dead_letter_failed_at
    ON dead_letter_events (failed_at);
