package com.blog.notification.subscription.relay

import com.blog.notification.common.kafka.KafkaTopics
import com.blog.notification.subscription.repository.SubscriptionOutboxJdbcDao
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper

// post.relay.PostOutboxRelay와 동일 패턴.
@Component
class SubscriptionOutboxRelay(
    private val outboxDao: SubscriptionOutboxJdbcDao,
    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val objectMapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedDelay = 1000)
    fun relay() {
        outboxDao.findPending(BATCH_SIZE).forEach { event ->
            try {
                val authorId = objectMapper.readTree(event.payload).get("authorId").asText()
                kafkaTemplate.send(KafkaTopics.SUBSCRIPTION_CHANGED, authorId, event.payload).get()
                outboxDao.markPublished(event.id)
            } catch (e: Exception) {
                log.error("Failed to relay subscription outbox event {}", event.id, e)
            }
        }
    }

    private companion object {
        const val BATCH_SIZE = 500
    }
}
