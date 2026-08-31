CREATE TABLE notification.fanout_dispatches (
    event_id UUID PRIMARY KEY,
    author_id BIGINT NOT NULL,
    cursor_user_id BIGINT,
    status VARCHAR(15) NOT NULL CHECK (status IN ('IN_PROGRESS', 'DONE')),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
