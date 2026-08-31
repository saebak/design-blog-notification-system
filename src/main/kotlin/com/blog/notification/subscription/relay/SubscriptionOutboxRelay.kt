package com.blog.notification.subscription.relay

import com.blog.notification.common.kafka.KafkaTopics
import com.blog.notification.common.outbox.OutboxEventRecord
import com.blog.notification.subscription.repository.SubscriptionOutboxJdbcDao
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper

// post.relay.PostOutboxRelay와 동일 패턴 — 논블로킹 발행 + inFlight 중복 제출 방지(docs/decisions.md §4).
@Component
class SubscriptionOutboxRelay(
    private val outboxDao: SubscriptionOutboxJdbcDao,
    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val objectMapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val inFlight = ConcurrentHashMap.newKeySet<UUID>()

    @Scheduled(fixedDelay = 1000)
    fun relay() {
        outboxDao.findPending(BATCH_SIZE)
            .filter { inFlight.add(it.id) }
            .forEach { event -> relayOne(event) }
    }

    private fun relayOne(event: OutboxEventRecord) {
        try {
            val authorId = objectMapper.readTree(event.payload).get("authorId").asText()
            kafkaTemplate.send(KafkaTopics.SUBSCRIPTION_CHANGED, authorId, event.payload)
                .whenComplete { _, ex ->
                    inFlight.remove(event.id)
                    if (ex == null) {
                        outboxDao.markPublished(event.id)
                    } else {
                        log.error("Failed to relay subscription outbox event {}", event.id, ex)
                    }
                }
        } catch (e: Exception) {
            inFlight.remove(event.id)
            log.error("Failed to relay subscription outbox event {}", event.id, e)
        }
    }

    private companion object {
        const val BATCH_SIZE = 500
    }
}
