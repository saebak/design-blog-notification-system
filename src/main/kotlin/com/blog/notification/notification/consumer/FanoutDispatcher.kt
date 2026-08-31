package com.blog.notification.notification.consumer

import com.blog.notification.common.kafka.KafkaTopics
import com.blog.notification.notification.consumer.dto.FanoutChunkRequestedMessage
import com.blog.notification.notification.consumer.dto.PostPublishedMessage
import com.blog.notification.notification.repository.FanoutDispatchJdbcDao
import com.blog.notification.notification.repository.SubscriberReadModelJdbcDao
import java.util.concurrent.TimeUnit
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper

@Component
class FanoutDispatcher(
    private val dispatchDao: FanoutDispatchJdbcDao,
    private val subscriberDao: SubscriberReadModelJdbcDao,
    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val objectMapper: ObjectMapper,
) {
    @KafkaListener(topics = [KafkaTopics.POST_PUBLISHED], groupId = "fanout-dispatcher")
    fun onMessage(payload: String) {
        val post = objectMapper.readValue(payload, PostPublishedMessage::class.java)
        dispatchDao.start(post.eventId, post.authorId)
        val state = dispatchDao.find(post.eventId) ?: error("Dispatch state was not created")
        if (state.done) return

        var cursor = state.cursorUserId
        var chunkIndex = 0
        while (true) {
            val userIds = subscriberDao.findUserIdsByAuthorAfter(post.authorId, cursor, CHUNK_SIZE)
            if (userIds.isEmpty()) {
                dispatchDao.markDone(post.eventId)
                return
            }
            val chunk = FanoutChunkRequestedMessage(post.eventId, post.postId, post.authorId, post.title, chunkIndex++, userIds)
            kafkaTemplate.send(KafkaTopics.FANOUT_CHUNK_REQUESTED, "${post.eventId}:$chunkIndex", objectMapper.writeValueAsString(chunk))
                .get(CHUNK_PUBLISH_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            cursor = userIds.last()
            dispatchDao.advance(post.eventId, cursor)
            if (userIds.size < CHUNK_SIZE) {
                dispatchDao.markDone(post.eventId)
                return
            }
        }
    }

    private companion object {
        const val CHUNK_SIZE = 1000
        const val CHUNK_PUBLISH_TIMEOUT_SECONDS = 30L
    }
}
