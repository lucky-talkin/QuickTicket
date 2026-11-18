-- Transactional outbox for reliable booking event publication.
CREATE TABLE IF NOT EXISTS outbox_events (
 id BIGSERIAL PRIMARY KEY,
 aggregate_type TEXT NOT NULL,
 aggregate_id BIGINT NOT NULL,
 event_type TEXT NOT NULL,
 payload JSONB NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 published_at TIMESTAMPTZ,
 attempts INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_outbox_pending
 ON outbox_events(created_at)
 WHERE published_at IS NULL;
