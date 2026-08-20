-- docs/database-design.md §3
CREATE TABLE subscription.subscriptions (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id        BIGINT NOT NULL,
    author_id      BIGINT NOT NULL,
    status         VARCHAR(10) NOT NULL DEFAULT 'ACTIVE'
                   CHECK (status IN ('ACTIVE', 'CANCELLED')),
    subscribed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    cancelled_at   TIMESTAMPTZ,

    CONSTRAINT uq_subscription_user_author UNIQUE (user_id, author_id)
);

-- 작가 기준 구독자 벌크 조회 (부트스트랩/백필용, FR-1.2)
CREATE INDEX idx_subscriptions_author_active
    ON subscription.subscriptions (author_id, id)
    WHERE status = 'ACTIVE';

-- "내 구독 목록" 조회
CREATE INDEX idx_subscriptions_user
    ON subscription.subscriptions (user_id)
    WHERE status = 'ACTIVE';

-- Post Context의 outbox_events와 동일한 구조 (event_type = 'SubscriptionChanged')
CREATE TABLE subscription.subscription_outbox_events (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type VARCHAR(50) NOT NULL,
    aggregate_id   BIGINT NOT NULL,
    event_type     VARCHAR(50) NOT NULL,
    payload        JSONB NOT NULL,
    status         VARCHAR(10) NOT NULL DEFAULT 'PENDING'
                   CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED')),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ
);

CREATE INDEX idx_subscription_outbox_pending ON subscription.subscription_outbox_events (created_at)
    WHERE status = 'PENDING';
