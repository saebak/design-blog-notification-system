package com.blog.notification

import com.blog.notification.post.service.PostService
import com.blog.notification.subscription.service.SubscriptionService
import com.blog.notification.user.NotificationChannel
import com.blog.notification.user.User
import com.blog.notification.user.repository.UserRepository
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate

// Outbox Relay -> Kafka -> Fan-out 컨슈머까지 전체 파이프라인이 실제로 동작하는지 확인하는 e2e 테스트.
@Import(TestcontainersConfiguration::class)
@SpringBootTest
class PostPublishedFanoutIntegrationTest {

    @Autowired
    lateinit var userRepository: UserRepository

    @Autowired
    lateinit var subscriptionService: SubscriptionService

    @Autowired
    lateinit var postService: PostService

    @Autowired
    lateinit var jdbcTemplate: NamedParameterJdbcTemplate

    @Test
    fun `구독 중이고 알림이 꺼져있지 않은 사용자에게만 알림이 생성된다`() {
        val author = userRepository.save(User(email = "author@test.com", name = "author"))
        val subscriber = userRepository.save(
            User(email = "sub@test.com", name = "sub", notificationChannel = NotificationChannel.PUSH),
        )
        val mutedSubscriber = userRepository.save(
            User(email = "muted@test.com", name = "muted", notificationChannel = NotificationChannel.MUTE),
        )

        subscriptionService.subscribe(requireNotNull(subscriber.id), requireNotNull(author.id))
        subscriptionService.subscribe(requireNotNull(mutedSubscriber.id), requireNotNull(author.id))

        // subscriber_read_model 동기화는 별도 비동기 파이프라인(subscription.changed 컨슈머)이라
        // subscribe() 호출이 끝났다고 바로 반영돼 있다는 보장이 없다(docs/architecture.md §2의
        // eventual consistency). 팬아웃이 정상 동작하는지 보려면 read model이 실제로 동기화된
        // 뒤에 publish해야 한다 — 이 gap 자체는 이 테스트가 검증할 대상이 아니다.
        awaitSubscriberReadModel(requireNotNull(author.id), setOf(subscriber.id, mutedSubscriber.id))

        val post = postService.create(requireNotNull(author.id), "title", "content")
        postService.publish(requireNotNull(post.id))

        val recipientIds = awaitNotificationRecipients(requireNotNull(post.id), expectedCount = 1)
        assertEquals(listOf(subscriber.id), recipientIds)

        // PUSH 채널 구독자는 PushDeliveryWorker가 SimulatedPushGatewayAdapter를 통해 발송까지 끝내야 한다.
        val deliveryStatus = awaitDeliveryLogStatus(requireNotNull(post.id), requireNotNull(subscriber.id))
        assertEquals("SENT", deliveryStatus)
    }

    private fun awaitDeliveryLogStatus(postId: Long, recipientId: Long): String {
        val deadline = System.currentTimeMillis() + 15_000
        while (System.currentTimeMillis() < deadline) {
            val statuses = jdbcTemplate.queryForList(
                """
                SELECT dl.status
                FROM notification.notification_delivery_log dl
                JOIN notification.notifications n ON n.id = dl.notification_id
                WHERE n.post_id = :postId AND n.recipient_id = :recipientId
                """.trimIndent(),
                MapSqlParameterSource("postId", postId).addValue("recipientId", recipientId),
                String::class.java,
            )
            if (statuses.isNotEmpty() && statuses[0] != "PENDING") return statuses[0]!!
            Thread.sleep(300)
        }
        throw AssertionError("Timed out waiting for delivery log status for post $postId, recipient $recipientId")
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

    private fun awaitNotificationRecipients(postId: Long, expectedCount: Int): List<Long> {
        val deadline = System.currentTimeMillis() + 15_000
        while (System.currentTimeMillis() < deadline) {
            val ids = jdbcTemplate.queryForList(
                "SELECT recipient_id FROM notification.notifications WHERE post_id = :postId ORDER BY recipient_id",
                MapSqlParameterSource("postId", postId),
                Long::class.java,
            ).filterNotNull()
            if (ids.size >= expectedCount) return ids
            Thread.sleep(300)
        }
        dumpDiagnostics(postId)
        throw AssertionError("Timed out waiting for notifications for post $postId")
    }

    private fun dumpDiagnostics(postId: Long) {
        println("=== DIAGNOSTICS ===")
        println(
            "post.outbox_events: " +
                jdbcTemplate.queryForList(
                    "SELECT id, event_type, status, payload FROM post.outbox_events",
                    MapSqlParameterSource(),
                ),
        )
        println(
            "subscription.subscription_outbox_events: " +
                jdbcTemplate.queryForList(
                    "SELECT id, event_type, status, payload FROM subscription.subscription_outbox_events",
                    MapSqlParameterSource(),
                ),
        )
        println(
            "notification.subscriber_read_model: " +
                jdbcTemplate.queryForList(
                    "SELECT author_id, user_id FROM notification.subscriber_read_model",
                    MapSqlParameterSource(),
                ),
        )
        println(
            "notification.notifications: " +
                jdbcTemplate.queryForList(
                    "SELECT recipient_id, post_id, author_id FROM notification.notifications",
                    MapSqlParameterSource(),
                ),
        )
        println("===================")
    }
}
