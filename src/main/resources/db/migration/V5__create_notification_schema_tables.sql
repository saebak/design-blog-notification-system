-- docs/database-design.md §4

-- §4.1 Subscription Context의 SubscriptionChanged 이벤트를 구독해 유지하는 로컬 복제본
CREATE TABLE notification.subscriber_read_model (
    author_id  BIGINT NOT NULL,
    user_id    BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (author_id, user_id)
);

-- §4.2
CREATE TABLE notification.notifications (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    recipient_id    BIGINT NOT NULL,
    source_event_id UUID NOT NULL,
    post_id         BIGINT NOT NULL,
    author_id       BIGINT NOT NULL,
    title           VARCHAR(200) NOT NULL,
    is_read         BOOLEAN NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    read_at         TIMESTAMPTZ,

    CONSTRAINT uq_recipient_event UNIQUE (recipient_id, source_event_id)
);

-- FR-5.1: 알림 목록 조회 (최신순, 키셋 페이지네이션)
CREATE INDEX idx_notifications_recipient_feed
    ON notification.notifications (recipient_id, id DESC);

-- FR-5.3: 미읽음 개수 조회
CREATE INDEX idx_notifications_unread
    ON notification.notifications (recipient_id)
    WHERE is_read = false;

-- §4.3 domain-design.md §5.3 DeliveryAttempt Entity의 물리 테이블
CREATE TABLE notification.notification_delivery_log (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    notification_id  BIGINT NOT NULL,
    channel          VARCHAR(10) NOT NULL CHECK (channel IN ('PUSH', 'EMAIL')),
    status           VARCHAR(15) NOT NULL DEFAULT 'PENDING'
                     CHECK (status IN ('PENDING', 'SENT', 'FAILED', 'DEAD_LETTER')),
    attempt_count    INT NOT NULL DEFAULT 0,
    last_attempt_at  TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_notification_channel UNIQUE (notification_id, channel)
);

-- 발송 워커의 재시도 대상 폴링
CREATE INDEX idx_notification_delivery_retry_queue
    ON notification.notification_delivery_log (channel, last_attempt_at)
    WHERE status IN ('PENDING', 'FAILED');
