package com.blog.notification.notification.consumer

import com.blog.notification.common.kafka.KafkaTopics
import com.blog.notification.notification.consumer.dto.PostPublishedMessage
import com.blog.notification.notification.repository.NotificationDeliveryLogJdbcDao
import com.blog.notification.notification.repository.NotificationInsert
import com.blog.notification.notification.repository.NotificationJdbcDao
import com.blog.notification.notification.repository.SubscriberReadModelJdbcDao
import com.blog.notification.user.NotificationChannel
import com.blog.notification.user.repository.UserRepository
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper

// post.published를 소비해 구독자 중 Mute가 아닌 사용자에게 알림을 생성한다.
// 청크 분산 없이 단일 컨슈머가 한 번에 처리하는 단순 버전 — 대량 트래픽 대응(Dispatcher/Chunk Worker
// 분리)은 스코프 밖, docs/architecture.md §4 참고.
@Component
class PostPublishedFanoutConsumer(
    private val subscriberReadModelDao: SubscriberReadModelJdbcDao,
    private val userRepository: UserRepository,
    private val notificationDao: NotificationJdbcDao,
    private val deliveryLogDao: NotificationDeliveryLogJdbcDao,
    private val objectMapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = [KafkaTopics.POST_PUBLISHED])
    fun onMessage(payload: String) {
        val message = objectMapper.readValue(payload, PostPublishedMessage::class.java)
        val subscriberIds = findSubscribersWithRetry(message.authorId)
        if (subscriberIds.isEmpty()) {
            log.warn(
                "No subscribers found for authorId={} after {} retries — either the author genuinely has none, " +
                    "or subscriber_read_model hasn't synced yet (docs/decisions.md §2)",
                message.authorId,
                EMPTY_SUBSCRIBER_RETRY_COUNT,
            )
            return
        }

        val eligibleUsers = userRepository.findByIdInAndNotificationChannelNot(subscriberIds, NotificationChannel.MUTE)

        eligibleUsers.forEach { user ->
            val notificationId = notificationDao.insertAndGetId(
                NotificationInsert(
                    recipientId = requireNotNull(user.id),
                    sourceEventId = message.eventId,
                    postId = message.postId,
                    authorId = message.authorId,
                    title = message.title,
                ),
            )
            // EMAIL 채널은 알림 row만 생성하고 실제 발송(delivery log)은 만들지 않는다
            // (Email 발송은 스코프 밖).
            if (user.notificationChannel == NotificationChannel.PUSH) {
                deliveryLogDao.insertPending(notificationId, "PUSH")
            }
        }
    }

    // subscriber_read_model 동기화(subscription.changed 컨슈머)는 이 컨슈머와 독립된 비동기
    // 파이프라인이라 순서를 보장하지 않는다(docs/decisions.md §2) — 구독 직후 곧바로 발행되면
    // Fan-out이 Read Model보다 먼저 도착해 구독자를 못 찾을 수 있다. 즉시 포기하는 대신 짧게
    // 재시도해 흔한 케이스(수 초 이내 동기화)를 흡수한다. 대량 outbox 백로그로 동기화가
    // 이 재시도 예산(약 4초)보다 더 오래 걸리는 극단적인 경우는 여전히 흡수하지 못한다 —
    // 그 경우는 §4(동기 블로킹 발행)를 고쳐 백로그 자체를 빨리 없애야 한다.
    private fun findSubscribersWithRetry(authorId: Long): List<Long> {
        repeat(EMPTY_SUBSCRIBER_RETRY_COUNT) { attempt ->
            val ids = subscriberReadModelDao.findUserIdsByAuthor(authorId)
            if (ids.isNotEmpty()) return ids
            if (attempt < EMPTY_SUBSCRIBER_RETRY_COUNT - 1) Thread.sleep(EMPTY_SUBSCRIBER_RETRY_DELAY_MS)
        }
        return emptyList()
    }

    private companion object {
        const val EMPTY_SUBSCRIBER_RETRY_COUNT = 5
        const val EMPTY_SUBSCRIBER_RETRY_DELAY_MS = 1000L
    }
}
