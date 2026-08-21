package com.blog.notification

import com.blog.notification.notification.gateway.PushGatewayPort
import com.blog.notification.post.service.PostService
import com.blog.notification.subscription.service.SubscriptionService
import com.blog.notification.user.NotificationChannel
import com.blog.notification.user.User
import com.blog.notification.user.repository.UserRepository
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.Primary
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate

// PushGatewayPort가 계속 실패할 때 재시도 후 DEAD_LETTER로 전이하는지 확인하는 e2e 테스트.
@Import(TestcontainersConfiguration::class, PushDeliveryDeadLetterIntegrationTest.AlwaysFailingPushGatewayConfig::class)
@SpringBootTest
class PushDeliveryDeadLetterIntegrationTest {

    @TestConfiguration
    class AlwaysFailingPushGatewayConfig {
        @Bean
        @Primary
        fun alwaysFailingPushGatewayPort(): PushGatewayPort = PushGatewayPort { _, _ -> false }
    }

    @Autowired
    lateinit var userRepository: UserRepository

    @Autowired
    lateinit var subscriptionService: SubscriptionService

    @Autowired
    lateinit var postService: PostService

    @Autowired
    lateinit var jdbcTemplate: NamedParameterJdbcTemplate

    @Test
    fun `Push 발송이 계속 실패하면 최대 재시도 후 DEAD_LETTER로 전이한다`() {
        val author = userRepository.save(User(email = "author2@test.com", name = "author2"))
        val subscriber = userRepository.save(
            User(email = "sub2@test.com", name = "sub2", notificationChannel = NotificationChannel.PUSH),
        )

        subscriptionService.subscribe(requireNotNull(subscriber.id), requireNotNull(author.id))
        awaitSubscriberReadModel(requireNotNull(author.id), setOf(subscriber.id))

        val post = postService.create(requireNotNull(author.id), "title", "content")
        postService.publish(requireNotNull(post.id))

        val (status, attemptCount) = awaitFinalDeliveryLog(requireNotNull(post.id), requireNotNull(subscriber.id))
        assertEquals("DEAD_LETTER", status)
        assertEquals(3, attemptCount)
    }

    private fun awaitSubscriberReadModel(authorId: Long, expectedUserIds: Set<Long?>) {
        val deadline = System.currentTimeMillis() + 15_000
        while (System.currentTimeMillis() < deadline) {
            val userIds = jdbcTemplate.queryForList(
                "SELECT user_id FROM notification.subscriber_read_model WHERE author_id = :authorId",
                MapSqlParameterSource("authorId", authorId),
                Long::class.java,
            ).filterNotNull().toSet()
            if (userIds == expectedUserIds) return
            Thread.sleep(300)
        }
        throw AssertionError("Timed out waiting for subscriber_read_model to sync for author $authorId")
    }

    private fun awaitFinalDeliveryLog(postId: Long, recipientId: Long): Pair<String, Int> {
        val deadline = System.currentTimeMillis() + 20_000
        while (System.currentTimeMillis() < deadline) {
            val rows = jdbcTemplate.queryForList(
                """
                SELECT dl.status, dl.attempt_count
                FROM notification.notification_delivery_log dl
                JOIN notification.notifications n ON n.id = dl.notification_id
                WHERE n.post_id = :postId AND n.recipient_id = :recipientId
                """.trimIndent(),
                MapSqlParameterSource("postId", postId).addValue("recipientId", recipientId),
            )
            if (rows.isNotEmpty() && rows[0]["status"] == "DEAD_LETTER") {
                return rows[0]["status"] as String to (rows[0]["attempt_count"] as Number).toInt()
            }
            Thread.sleep(300)
        }
        throw AssertionError("Timed out waiting for DEAD_LETTER delivery log for post $postId, recipient $recipientId")
    }
}
