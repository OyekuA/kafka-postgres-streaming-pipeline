CREATE TABLE IF NOT EXISTS transaction_events (
    event_id        UUID PRIMARY KEY,
    event_timestamp TIMESTAMPTZ     NOT NULL,
    account_id      VARCHAR(64)     NOT NULL,
    amount          DECIMAL(18, 2)  NOT NULL,
    transaction_type VARCHAR(32)    NOT NULL,
    ingested_at     TIMESTAMPTZ     DEFAULT NOW()
);
