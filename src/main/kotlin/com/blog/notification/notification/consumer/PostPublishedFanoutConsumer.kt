package com.blog.notification.notification.consumer

import com.blog.notification.common.kafka.KafkaTopics
import com.blog.notification.notification.consumer.dto.PostPublishedMessage
import com.blog.notification.notification.repository.NotificationDeliveryLogJdbcDao
import com.blog.notification.notification.repository.NotificationInsert
import com.blog.notification.notification.repository.NotificationJdbcDao
import com.blog.notification.notification.repository.SubscriberReadModelJdbcDao
import com.blog.notification.user.NotificationChannel
import com.blog.notification.user.repository.UserRepository
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
    @KafkaListener(topics = [KafkaTopics.POST_PUBLISHED])
    fun onMessage(payload: String) {
        val message = objectMapper.readValue(payload, PostPublishedMessage::class.java)
        val subscriberIds = subscriberReadModelDao.findUserIdsByAuthor(message.authorId)
        if (subscriberIds.isEmpty()) return

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
}
