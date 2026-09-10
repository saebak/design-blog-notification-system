package com.blog.notification

import com.blog.notification.notification.consumer.FanoutDispatcher
import com.blog.notification.notification.consumer.dto.PostPublishedMessage
import com.blog.notification.notification.repository.FanoutDispatchJdbcDao
import java.time.Instant
import java.util.UUID
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import tools.jackson.databind.ObjectMapper

@Import(TestcontainersConfiguration::class)
@SpringBootTest
class FanoutChunkBoundaryIntegrationTest {

    @Autowired lateinit var dispatcher: FanoutDispatcher
    @Autowired lateinit var dispatchDao: FanoutDispatchJdbcDao
    @Autowired lateinit var objectMapper: ObjectMapper
    @Autowired lateinit var jdbcTemplate: NamedParameterJdbcTemplate

    @Test
    fun `1001 subscribers are split into two chunks and duplicate event is idempotent`() {
        val suffix = UUID.randomUUID().toString()
        val authorId = insertUser("chunk-author-$suffix@test.com", "PUSH")
        insertSubscribers(authorId, suffix, 1_001)
        val eventId = UUID.randomUUID()
        val postId = authorId + 1_000_000L
        val payload = objectMapper.writeValueAsString(
            PostPublishedMessage(eventId, postId, authorId, "chunk boundary", Instant.now()),
        )

        dispatcher.onMessage(payload)
        awaitNotificationCount(postId, 1_001)

        val completed = requireNotNull(dispatchDao.find(eventId))
        assertEquals("DONE", completed.status)
        assertEquals(2, completed.nextChunkIndex)
        assertEquals(1_001, notificationCount(postId))

        // Kafka가 같은 post.published 이벤트를 재전달해도 DONE 상태와 알림 unique key가 중복을 막는다.
        dispatcher.onMessage(payload)
        Thread.sleep(500)
        assertEquals(1_001, notificationCount(postId))
    }

    @Test
    fun `dispatcher resumes after the persisted cursor and chunk index`() {
        val suffix = UUID.randomUUID().toString()
        val authorId = insertUser("resume-author-$suffix@test.com", "PUSH")
        insertSubscribers(authorId, suffix, 1_001)
        val subscriberIds = jdbcTemplate.queryForList(
            "SELECT user_id FROM notification.subscriber_read_model WHERE author_id = :authorId ORDER BY user_id",
            MapSqlParameterSource("authorId", authorId),
            Long::class.java,
        ).filterNotNull()
        val eventId = UUID.randomUUID()
        val postId = authorId + 2_000_000L
        val message = PostPublishedMessage(eventId, postId, authorId, "resume", Instant.now())

        // 첫 번째 1,000명 청크의 Kafka ACK 뒤 cursor가 저장된 시점을 재현한다.
        dispatchDao.start(eventId, postId, authorId, message.title)
        dispatchDao.advance(eventId, subscriberIds[999])

        dispatcher.onMessage(objectMapper.writeValueAsString(message))
        awaitNotificationCount(postId, 1)

        val completed = requireNotNull(dispatchDao.find(eventId))
        assertEquals("DONE", completed.status)
        assertEquals(2, completed.nextChunkIndex)
        assertEquals(listOf(subscriberIds.last()), notificationRecipients(postId))
    }

    private fun insertUser(email: String, channel: String): Long = jdbcTemplate.queryForObject(
        "INSERT INTO users (email, name, notification_channel) VALUES (:email, 'test', :channel) RETURNING id",
        MapSqlParameterSource("email", email).addValue("channel", channel),
        Long::class.java,
    )!!

    private fun insertSubscribers(authorId: Long, suffix: String, count: Int) {
        jdbcTemplate.update(
            """
            INSERT INTO users (email, name, notification_channel)
            SELECT 'chunk-$suffix-' || value || '@test.com', 'subscriber-' || value, 'EMAIL'
            FROM generate_series(1, :count) AS value
            """.trimIndent(),
            MapSqlParameterSource("count", count),
        )
        jdbcTemplate.update(
            """
            INSERT INTO notification.subscriber_read_model (author_id, user_id)
            SELECT :authorId, id FROM users WHERE email LIKE :emailPattern
            """.trimIndent(),
            MapSqlParameterSource("authorId", authorId).addValue("emailPattern", "chunk-$suffix-%"),
        )
        jdbcTemplate.update(
            """
            INSERT INTO subscription.subscriptions (user_id, author_id, status)
            SELECT id, :authorId, 'ACTIVE' FROM users WHERE email LIKE :emailPattern
            """.trimIndent(),
            MapSqlParameterSource("authorId", authorId).addValue("emailPattern", "chunk-$suffix-%"),
        )
    }

    private fun awaitNotificationCount(postId: Long, expected: Int) {
        val deadline = System.currentTimeMillis() + 30_000
        while (System.currentTimeMillis() < deadline) {
            if (notificationCount(postId) == expected) return
            Thread.sleep(200)
        }
        throw AssertionError("Timed out waiting for $expected notifications for post $postId; actual=${notificationCount(postId)}")
    }

    private fun notificationCount(postId: Long): Int = jdbcTemplate.queryForObject(
        "SELECT count(*) FROM notification.notifications WHERE post_id = :postId",
        MapSqlParameterSource("postId", postId),
        Int::class.java,
    )!!

    private fun notificationRecipients(postId: Long): List<Long> = jdbcTemplate.queryForList(
        "SELECT recipient_id FROM notification.notifications WHERE post_id = :postId ORDER BY recipient_id",
        MapSqlParameterSource("postId", postId),
        Long::class.java,
    ).filterNotNull()
}
