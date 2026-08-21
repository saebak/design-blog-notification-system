package com.blog.notification.post.relay

import com.blog.notification.common.kafka.KafkaTopics
import com.blog.notification.post.repository.OutboxEventJdbcDao
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper

// PENDING 이벤트를 폴링해 Kafka에 발행. 실패하면 상태를 그대로 두고 다음 폴링에서 재시도한다(at-least-once).
@Component
class PostOutboxRelay(
    private val outboxDao: OutboxEventJdbcDao,
    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val objectMapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedDelay = 1000)
    fun relay() {
        outboxDao.findPending(BATCH_SIZE).forEach { event ->
            try {
                val authorId = objectMapper.readTree(event.payload).get("authorId").asText()
                kafkaTemplate.send(KafkaTopics.POST_PUBLISHED, authorId, event.payload).get()
                outboxDao.markPublished(event.id)
            } catch (e: Exception) {
                log.error("Failed to relay post outbox event {}", event.id, e)
            }
        }
    }

    private companion object {
        const val BATCH_SIZE = 500
    }
}
