package com.blog.notification.notification.consumer

import com.blog.notification.common.kafka.KafkaTopics
import com.blog.notification.notification.consumer.dto.FanoutChunkRequestedMessage
import com.blog.notification.notification.repository.NotificationDeliveryLogJdbcDao
import com.blog.notification.notification.repository.NotificationInsert
import com.blog.notification.notification.repository.NotificationJdbcDao
import com.blog.notification.user.NotificationChannel
import com.blog.notification.user.repository.UserRepository
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import tools.jackson.databind.ObjectMapper

@Component
class FanoutChunkWorker(
    private val userRepository: UserRepository,
    private val notificationDao: NotificationJdbcDao,
    private val deliveryLogDao: NotificationDeliveryLogJdbcDao,
    private val objectMapper: ObjectMapper,
) {
    @KafkaListener(topics = [KafkaTopics.FANOUT_CHUNK_REQUESTED], groupId = "fanout-chunk-worker", concurrency = "6")
    @Transactional
    fun onMessage(payload: String) {
        val chunk = objectMapper.readValue(payload, FanoutChunkRequestedMessage::class.java)
        val users = userRepository.findByIdInAndNotificationChannelNot(chunk.userIds, NotificationChannel.MUTE)
        users.forEach { user ->
            val notificationId = notificationDao.insertAndGetId(
                NotificationInsert(requireNotNull(user.id), chunk.eventId, chunk.postId, chunk.authorId, chunk.title),
            )
            if (user.notificationChannel == NotificationChannel.PUSH) deliveryLogDao.insertPending(notificationId)
        }
    }
}
