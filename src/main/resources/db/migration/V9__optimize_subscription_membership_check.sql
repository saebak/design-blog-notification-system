-- FanoutDispatcher의 exact membership gate는 ACTIVE 원본 구독과 Subscriber Read Model을
-- author_id 안에서 user_id 순서로 비교한다. 기존 (author_id, id) 인덱스는 백필 키셋용이고,
-- (user_id, author_id) unique 인덱스는 인기 작가별 범위 스캔에 맞지 않는다.
CREATE INDEX idx_subscriptions_author_active_membership
    ON subscription.subscriptions (author_id, user_id)
    WHERE status = 'ACTIVE';
