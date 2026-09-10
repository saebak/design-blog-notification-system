ALTER TABLE notification.notification_delivery_log
    DROP CONSTRAINT notification_delivery_log_status_check;

ALTER TABLE notification.notification_delivery_log
    ADD COLUMN claim_token UUID,
    ADD COLUMN claimed_until TIMESTAMPTZ,
    ADD CONSTRAINT notification_delivery_log_status_check
        CHECK (status IN ('PENDING', 'PROCESSING', 'SENT', 'FAILED', 'DEAD_LETTER'));

CREATE INDEX idx_notification_delivery_expired_claim
    ON notification.notification_delivery_log (claimed_until)
    WHERE status = 'PROCESSING';
