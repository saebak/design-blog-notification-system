package com.blog.notification

import com.blog.notification.notification.repository.SubscriberReadModelJdbcDao
import com.blog.notification.post.service.PostService
import com.blog.notification.subscription.service.SubscriptionService
import com.blog.notification.user.User
import com.blog.notification.user.repository.UserRepository
import java.util.UUID
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.testcontainers.DockerClientFactory
import org.testcontainers.kafka.KafkaContainer

@Import(TestcontainersConfiguration::class)
@SpringBootTest
class KafkaOutageRecoveryIntegrationTest {

    @Autowired lateinit var kafkaContainer: KafkaContainer
    @Autowired lateinit var userRepository: UserRepository
    @Autowired lateinit var subscriptionService: SubscriptionService
    @Autowired lateinit var postService: PostService
    @Autowired lateinit var subscriberDao: SubscriberReadModelJdbcDao
    @Autowired lateinit var jdbcTemplate: NamedParameterJdbcTemplate

    @Test
    fun `publish commits during kafka outage and pipeline catches up after recovery`() {
        val suffix = UUID.randomUUID().toString()
        val author = userRepository.save(User(email = "kafka-author-$suffix@test.com", name = "author"))
        val subscriber = userRepository.save(User(email = "kafka-subscriber-$suffix@test.com", name = "subscriber"))
        val authorId = requireNotNull(author.id)
        val subscriberId = requireNotNull(subscriber.id)
        subscriptionService.subscribe(subscriberId, authorId)
        await("subscriber read model") {
            subscriberDao.findUserIdsByAuthor(authorId).contains(subscriberId)
        }
        val post = postService.create(authorId, "kafka outage", "content")
        val postId = requireNotNull(post.id)
        val docker = DockerClientFactory.instance().client()

        docker.pauseContainerCmd(kafkaContainer.containerId).exec()
        try {
            assertEquals("PUBLISHED", postService.publish(postId).status.name)
            assertEquals("PENDING", postOutboxStatus(postId))
        } finally {
            docker.unpauseContainerCmd(kafkaContainer.containerId).exec()
        }

        await("outbox publication") { postOutboxStatus(postId) == "PUBLISHED" }
        await("fan-out completion") { notificationCount(postId) == 1 }
    }

    private fun postOutboxStatus(postId: Long): String = jdbcTemplate.queryForObject(
        "SELECT status FROM post.outbox_events WHERE aggregate_id = :postId AND event_type = 'PostPublished'",
        MapSqlParameterSource("postId", postId),
        String::class.java,
    )!!

    private fun notificationCount(postId: Long): Int = jdbcTemplate.queryForObject(
        "SELECT count(*) FROM notification.notifications WHERE post_id = :postId",
        MapSqlParameterSource("postId", postId),
        Int::class.java,
    )!!

    private fun await(description: String, timeoutMillis: Long = 30_000, condition: () -> Boolean) {
        val deadline = System.currentTimeMillis() + timeoutMillis
        while (System.currentTimeMillis() < deadline) {
            if (condition()) return
            Thread.sleep(200)
        }
        throw AssertionError("Timed out waiting for $description")
    }
}
