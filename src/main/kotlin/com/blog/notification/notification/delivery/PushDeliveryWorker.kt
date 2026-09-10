package com.blog.notification.notification.delivery

import com.blog.notification.notification.gateway.PushGatewayPort
import com.blog.notification.notification.operations.NotificationOperationsMetrics
import com.blog.notification.notification.repository.NotificationDeliveryLogJdbcDao
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

// post.relay.PostOutboxRelay와 동일한 패턴 — Kafka 토픽 대신 notification_delivery_log를
// 폴링해 재시도한다(docs/decisions.md §7). 실패하면 attempt_count를 늘려 백오프하고,
// 최대 재시도 초과 시 DEAD_LETTER로 전이한다.
@Component
class PushDeliveryWorker(
    private val deliveryLogDao: NotificationDeliveryLogJdbcDao,
    private val pushGatewayPort: PushGatewayPort,
    private val metrics: NotificationOperationsMetrics,
    @Value("\${notification.delivery.worker.enabled:true}") private val enabled: Boolean,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedDelay = 1000)
    fun retry() {
        if (!enabled) return
        val attempts = deliveryLogDao.claimDue(BATCH_SIZE, MAX_ATTEMPTS, CLAIM_LEASE_SECONDS)
        metrics.recordDeliveryClaimed(attempts.size)
        attempts.forEach { attempt ->
            val sent = try {
                pushGatewayPort.send(attempt.id, attempt.recipientId, attempt.title)
            } catch (e: Exception) {
                log.error("Push gateway call failed for delivery log {}", attempt.id, e)
                false
            }
            if (sent) {
                if (deliveryLogDao.markSent(attempt.id, attempt.claimToken)) metrics.recordDeliverySent()
            } else {
                val status = deliveryLogDao.recordFailedAttempt(attempt.id, attempt.claimToken, MAX_ATTEMPTS)
                if (status != null) metrics.recordDeliveryFailed(status == "DEAD_LETTER")
            }
        }
    }

    private companion object {
        const val BATCH_SIZE = 500
        const val MAX_ATTEMPTS = 3
        const val CLAIM_LEASE_SECONDS = 30L
    }
}
