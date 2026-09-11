package com.blog.notification.notification.consumer

import com.blog.notification.common.kafka.KafkaTopics
import com.blog.notification.notification.consumer.dto.FanoutChunkRequestedMessage
import com.blog.notification.notification.consumer.dto.PostPublishedMessage
import com.blog.notification.notification.operations.NotificationOperationsMetrics
import com.blog.notification.notification.repository.FanoutDispatchJdbcDao
import com.blog.notification.notification.repository.FanoutDispatchState
import com.blog.notification.notification.repository.SubscriberReadModelJdbcDao
import java.util.concurrent.TimeUnit
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper

@Component
class FanoutDispatcher(
    private val dispatchDao: FanoutDispatchJdbcDao,
    private val subscriberDao: SubscriberReadModelJdbcDao,
    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val objectMapper: ObjectMapper,
    private val metrics: NotificationOperationsMetrics,
    @Value("\${notification.fanout.retry-enabled:true}") private val retryEnabled: Boolean,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = [KafkaTopics.POST_PUBLISHED], groupId = "fanout-dispatcher")
    fun onMessage(payload: String) {
        val post = objectMapper.readValue(payload, PostPublishedMessage::class.java)
        dispatchDao.start(post.eventId, post.postId, post.authorId, post.title)
        val state = dispatchDao.find(post.eventId) ?: error("Dispatch state was not created")
        when (state.status) {
            "DONE", "FAILED", "WAITING_RETRY" -> return
        }
        dispatch(state)
    }

    @Scheduled(fixedDelayString = "\${notification.fanout.retry-poll-delay-ms:1000}")
    fun retryDueDispatches() {
        if (!retryEnabled) return
        dispatchDao.findDueRetries(RETRY_BATCH_SIZE).forEach { eventId ->
            if (!dispatchDao.claimRetry(eventId)) return@forEach
            val state = dispatchDao.find(eventId) ?: return@forEach
            runCatching { dispatch(state) }
                .onFailure { error ->
                    log.error("Retryable fan-out dispatch failed for eventId={}", eventId, error)
                    scheduleRetry(state, "Dispatch error: ${error.message}")
                }
        }
    }

    private fun dispatch(state: FanoutDispatchState) {
        var cursor = state.cursorUserId
        var chunkIndex = state.nextChunkIndex
        if (cursor == null && !subscriberDao.isSynchronizedWithActiveSubscriptions(state.authorId)) {
            scheduleRetry(state, "Subscriber read model membership is not synchronized")
            return
        }
        while (true) {
            val userIds = subscriberDao.findUserIdsByAuthorAfter(state.authorId, cursor, CHUNK_SIZE)
            if (userIds.isEmpty()) {
                dispatchDao.markDone(state.eventId)
                return
            }
            val chunk = FanoutChunkRequestedMessage(
                state.eventId,
                state.postId,
                state.authorId,
                state.title,
                chunkIndex++,
                userIds,
            )
            kafkaTemplate.send(KafkaTopics.FANOUT_CHUNK_REQUESTED, "${state.eventId}:$chunkIndex", objectMapper.writeValueAsString(chunk))
                .get(CHUNK_PUBLISH_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            metrics.recordFanoutChunkDispatched()
            cursor = userIds.last()
            dispatchDao.advance(state.eventId, cursor)
            if (userIds.size < CHUNK_SIZE) {
                dispatchDao.markDone(state.eventId)
                return
            }
        }
    }

    private fun scheduleRetry(state: FanoutDispatchState, reason: String) {
        val delaySeconds = (1L shl state.retryCount.coerceAtMost(MAX_BACKOFF_EXPONENT))
        val status = dispatchDao.scheduleRetry(state.eventId, delaySeconds, MAX_EMPTY_READ_MODEL_RETRIES, reason)
        if (status == "FAILED") {
            metrics.recordFanoutFailed()
            log.error(
                "Fan-out dispatch exhausted retries and remains recoverable as FAILED: eventId={}, authorId={}",
                state.eventId,
                state.authorId,
            )
        } else {
            metrics.recordFanoutDeferred()
            log.warn(
                "Fan-out dispatch deferred because the subscriber read model is not ready: eventId={}, retryIn={}s",
                state.eventId,
                delaySeconds,
            )
        }
    }

    private companion object {
        const val CHUNK_SIZE = 1000
        const val CHUNK_PUBLISH_TIMEOUT_SECONDS = 30L
        const val RETRY_BATCH_SIZE = 100
        const val MAX_EMPTY_READ_MODEL_RETRIES = 8
        const val MAX_BACKOFF_EXPONENT = 5
    }
}
