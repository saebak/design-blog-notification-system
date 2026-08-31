package com.blog.notification.post.relay

import com.blog.notification.common.kafka.KafkaTopics
import com.blog.notification.common.outbox.OutboxEventRecord
import com.blog.notification.post.repository.OutboxEventJdbcDao
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper

// PENDING 이벤트를 폴링해 Kafka에 발행. 실패하면 상태를 그대로 두고 다음 폴링에서 재시도한다(at-least-once).
// 발행 자체는 논블로킹(docs/decisions.md §4) — ack을 기다리지 않고 콜백에서 상태를 갱신한다.
// inFlight로 아직 ack이 안 돌아온 이벤트를 추적해 같은 행을 매초 중복 제출하지 않는다 — 이게
// 없으면 브로커 장애 중 매 폴링(1초)마다 같은 PENDING 행을 다시 제출하게 되어, 브로커 복구 시
// 쌓여있던 중복 제출분이 한꺼번에 타임아웃 폭주를 일으킨다(실측: 짧은 장애에도 수만 건).
@Component
class PostOutboxRelay(
    private val outboxDao: OutboxEventJdbcDao,
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
            kafkaTemplate.send(KafkaTopics.POST_PUBLISHED, authorId, event.payload)
                .whenComplete { _, ex ->
                    inFlight.remove(event.id)
                    if (ex == null) {
                        outboxDao.markPublished(event.id)
                    } else {
                        log.error("Failed to relay post outbox event {}", event.id, ex)
                    }
                }
        } catch (e: Exception) {
            inFlight.remove(event.id)
            log.error("Failed to relay post outbox event {}", event.id, e)
        }
    }

    private companion object {
        const val BATCH_SIZE = 500
    }
}
