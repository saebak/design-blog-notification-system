-- docs/database-design.md §1
CREATE TABLE users (
    id                    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email                 VARCHAR(255) NOT NULL,
    name                  VARCHAR(100) NOT NULL,
    notification_channel  VARCHAR(10) NOT NULL DEFAULT 'PUSH'
                          CHECK (notification_channel IN ('PUSH', 'EMAIL', 'MUTE')),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_users_email UNIQUE (email)
);
