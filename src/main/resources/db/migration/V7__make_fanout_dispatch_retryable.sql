ALTER TABLE notification.fanout_dispatches
    DROP CONSTRAINT fanout_dispatches_status_check;

ALTER TABLE notification.fanout_dispatches
    ADD COLUMN post_id BIGINT,
    ADD COLUMN title VARCHAR(200),
    ADD COLUMN retry_count INT NOT NULL DEFAULT 0,
    ADD COLUMN next_chunk_index INT NOT NULL DEFAULT 0,
    ADD COLUMN next_retry_at TIMESTAMPTZ,
    ADD COLUMN last_error VARCHAR(500),
    ADD CONSTRAINT fanout_dispatches_status_check
        CHECK (status IN ('IN_PROGRESS', 'WAITING_RETRY', 'DONE', 'FAILED'));

CREATE INDEX idx_fanout_dispatches_retry
    ON notification.fanout_dispatches (next_retry_at)
    WHERE status = 'WAITING_RETRY';
