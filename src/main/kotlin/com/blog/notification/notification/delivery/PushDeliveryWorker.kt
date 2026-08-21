package com.blog.notification.notification.delivery

import com.blog.notification.notification.gateway.PushGatewayPort
import com.blog.notification.notification.repository.NotificationDeliveryLogJdbcDao
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

// post.relay.PostOutboxRelay와 동일한 패턴 — Kafka 토픽 대신 notification_delivery_log를
// 폴링해 재시도한다(docs/decisions.md §7). 실패하면 attempt_count를 늘려 백오프하고,
// 최대 재시도 초과 시 DEAD_LETTER로 전이한다.
@Component
class PushDeliveryWorker(
    private val deliveryLogDao: NotificationDeliveryLogJdbcDao,
    private val pushGatewayPort: PushGatewayPort,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedDelay = 1000)
    fun retry() {
        deliveryLogDao.findDue(BATCH_SIZE, MAX_ATTEMPTS).forEach { attempt ->
            val sent = try {
                pushGatewayPort.send(attempt.recipientId, attempt.title)
            } catch (e: Exception) {
                log.error("Push gateway call failed for delivery log {}", attempt.id, e)
                false
            }
            if (sent) {
                deliveryLogDao.markSent(attempt.id)
            } else {
                deliveryLogDao.recordFailedAttempt(attempt.id, MAX_ATTEMPTS)
            }
        }
    }

    private companion object {
        const val BATCH_SIZE = 500
        const val MAX_ATTEMPTS = 3
    }
}
