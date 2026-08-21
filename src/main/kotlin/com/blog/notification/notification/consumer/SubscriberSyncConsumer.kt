package com.blog.notification.notification.consumer

import com.blog.notification.common.kafka.KafkaTopics
import com.blog.notification.notification.consumer.dto.SubscriptionChangedMessage
import com.blog.notification.notification.repository.SubscriberReadModelJdbcDao
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper

// subscription.changed를 소비해 Subscriber Read Model을 최신 상태로 유지한다.
@Component
class SubscriberSyncConsumer(
    private val subscriberReadModelDao: SubscriberReadModelJdbcDao,
    private val objectMapper: ObjectMapper,
) {
    @KafkaListener(topics = [KafkaTopics.SUBSCRIPTION_CHANGED])
    fun onMessage(payload: String) {
        val message = objectMapper.readValue(payload, SubscriptionChangedMessage::class.java)
        when (message.status) {
            "ACTIVE" -> subscriberReadModelDao.upsert(message.authorId, message.userId)
            "CANCELLED" -> subscriberReadModelDao.delete(message.authorId, message.userId)
        }
    }
}
