package com.blog.notification

import com.blog.notification.notification.consumer.FanoutDispatcher
import com.blog.notification.notification.consumer.dto.PostPublishedMessage
import com.blog.notification.notification.repository.FanoutDispatchJdbcDao
import com.blog.notification.notification.repository.SubscriberReadModelJdbcDao
import com.blog.notification.user.NotificationChannel
import com.blog.notification.user.User
import com.blog.notification.user.repository.UserRepository
import java.time.Instant
import java.util.UUID
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import tools.jackson.databind.ObjectMapper

@Import(TestcontainersConfiguration::class)
@SpringBootTest
class FanoutDispatcherRetryIntegrationTest {

    @Autowired lateinit var dispatcher: FanoutDispatcher
    @Autowired lateinit var dispatchDao: FanoutDispatchJdbcDao
    @Autowired lateinit var subscriberReadModelDao: SubscriberReadModelJdbcDao
    @Autowired lateinit var userRepository: UserRepository
    @Autowired lateinit var objectMapper: ObjectMapper
    @Autowired lateinit var jdbcTemplate: NamedParameterJdbcTemplate

    @Test
    fun `empty read model is persisted for retry and completes after synchronization`() {
        val author = userRepository.save(User(email = "retry-author@test.com", name = "author"))
        val subscriber = userRepository.save(
            User(email = "retry-subscriber@test.com", name = "subscriber", notificationChannel = NotificationChannel.PUSH),
        )
        val eventId = UUID.randomUUID()
        val postId = 900_001L
        val message = PostPublishedMessage(
            eventId = eventId,
            postId = postId,
            authorId = requireNotNull(author.id),
            title = "retryable fan-out",
            publishedAt = Instant.now(),
        )

        insertActiveSubscription(requireNotNull(author.id), requireNotNull(subscriber.id))

        // SubscriberSyncConsumer의 Kafka 타이밍을 사용하지 않고 Dispatcher를 직접 호출해
        // read model이 비어 있는 상태를 결정적으로 만든다.
        dispatcher.onMessage(objectMapper.writeValueAsString(message))

        val waiting = requireNotNull(dispatchDao.find(eventId))
        assertEquals("WAITING_RETRY", waiting.status)
        assertEquals(1, waiting.retryCount)
        assertTrue(waiting.nextRetryAt != null)

        subscriberReadModelDao.upsert(requireNotNull(author.id), requireNotNull(subscriber.id))

        val deadline = System.currentTimeMillis() + 15_000
        while (System.currentTimeMillis() < deadline) {
            dispatcher.retryDueDispatches()
            if (notificationRecipients(postId) == listOf(subscriber.id)) break
            Thread.sleep(200)
        }

        assertEquals(listOf(subscriber.id), notificationRecipients(postId))
        assertEquals("DONE", dispatchDao.find(eventId)?.status)
    }

    @Test
    fun `partially synchronized read model waits before publishing any chunk`() {
        val suffix = UUID.randomUUID().toString()
        val author = userRepository.save(User(email = "partial-author-$suffix@test.com", name = "author"))
        val first = userRepository.save(User(email = "partial-first-$suffix@test.com", name = "first"))
        val second = userRepository.save(User(email = "partial-second-$suffix@test.com", name = "second"))
        val authorId = requireNotNull(author.id)
        val firstId = requireNotNull(first.id)
        val secondId = requireNotNull(second.id)
        insertActiveSubscription(authorId, firstId)
        insertActiveSubscription(authorId, secondId)
        subscriberReadModelDao.upsert(authorId, firstId)

        val eventId = UUID.randomUUID()
        val postId = 900_002L
        val message = PostPublishedMessage(eventId, postId, authorId, "partial read model", Instant.now())
        dispatcher.onMessage(objectMapper.writeValueAsString(message))

        assertEquals("WAITING_RETRY", dispatchDao.find(eventId)?.status)
        assertTrue(notificationRecipients(postId).isEmpty())

        subscriberReadModelDao.upsert(authorId, secondId)
        val deadline = System.currentTimeMillis() + 15_000
        while (System.currentTimeMillis() < deadline) {
            dispatcher.retryDueDispatches()
            if (notificationRecipients(postId).size == 2) break
            Thread.sleep(200)
        }

        assertEquals(listOf(firstId, secondId).sorted(), notificationRecipients(postId))
        assertEquals("DONE", dispatchDao.find(eventId)?.status)
    }

    @Test
    fun `equal counts with different members still waits for exact synchronization`() {
        val suffix = UUID.randomUUID().toString()
        val author = userRepository.save(User(email = "set-author-$suffix@test.com", name = "author"))
        val first = userRepository.save(User(email = "set-first-$suffix@test.com", name = "first"))
        val second = userRepository.save(User(email = "set-second-$suffix@test.com", name = "second"))
        val stale = userRepository.save(User(email = "set-stale-$suffix@test.com", name = "stale"))
        val authorId = requireNotNull(author.id)
        val firstId = requireNotNull(first.id)
        val secondId = requireNotNull(second.id)
        val staleId = requireNotNull(stale.id)
        insertActiveSubscription(authorId, firstId)
        insertActiveSubscription(authorId, secondId)
        subscriberReadModelDao.upsert(authorId, firstId)
        subscriberReadModelDao.upsert(authorId, staleId)

        val eventId = UUID.randomUUID()
        val postId = 900_003L
        val message = PostPublishedMessage(eventId, postId, authorId, "equal count mismatch", Instant.now())
        dispatcher.onMessage(objectMapper.writeValueAsString(message))

        assertEquals("WAITING_RETRY", dispatchDao.find(eventId)?.status)
        assertTrue(notificationRecipients(postId).isEmpty())

        subscriberReadModelDao.delete(authorId, staleId)
        subscriberReadModelDao.upsert(authorId, secondId)
        val deadline = System.currentTimeMillis() + 15_000
        while (System.currentTimeMillis() < deadline) {
            dispatcher.retryDueDispatches()
            if (notificationRecipients(postId).size == 2) break
            Thread.sleep(200)
        }

        assertEquals(listOf(firstId, secondId).sorted(), notificationRecipients(postId))
        assertEquals("DONE", dispatchDao.find(eventId)?.status)
    }

    private fun insertActiveSubscription(authorId: Long, userId: Long) {
        jdbcTemplate.update(
            "INSERT INTO subscription.subscriptions (user_id, author_id, status) VALUES (:userId, :authorId, 'ACTIVE')",
            MapSqlParameterSource("userId", userId).addValue("authorId", authorId),
        )
    }

    private fun notificationRecipients(postId: Long): List<Long> = jdbcTemplate.queryForList(
        "SELECT recipient_id FROM notification.notifications WHERE post_id = :postId ORDER BY recipient_id",
        MapSqlParameterSource("postId", postId),
        Long::class.java,
    ).filterNotNull()
}
