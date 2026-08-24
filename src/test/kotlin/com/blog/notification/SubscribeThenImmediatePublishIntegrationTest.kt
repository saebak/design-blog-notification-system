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

// docs/decisions.md §2 — subscriber_read_model 동기화와 Fan-out은 서로 다른 비동기 파이프라인이라
// 순서를 보장하지 않는다. 이 테스트는 그 gap을 일부러 피하지 않고 "구독 직후 즉시 발행"을 그대로
// 재현해, PostPublishedFanoutConsumer의 재시도(findSubscribersWithRetry)가 이 흔한 케이스를
// 흡수하는지 검증한다 — PostPublishedFanoutIntegrationTest는 반대로 이 레이스를 피해서 만든
// 안정적인 happy-path 테스트이니 혼동하지 말 것.
@Import(TestcontainersConfiguration::class)
@SpringBootTest
class SubscribeThenImmediatePublishIntegrationTest {

    @Autowired
    lateinit var userRepository: UserRepository

    @Autowired
    lateinit var subscriptionService: SubscriptionService

    @Autowired
    lateinit var postService: PostService

    @Autowired
    lateinit var jdbcTemplate: NamedParameterJdbcTemplate

    @Test
    fun `구독 직후 read model 동기화를 기다리지 않고 바로 발행해도 재시도로 알림이 생성된다`() {
        val author = userRepository.save(User(email = "immediate-author@test.com", name = "author"))
        val subscriber = userRepository.save(
            User(email = "immediate-sub@test.com", name = "sub", notificationChannel = NotificationChannel.PUSH),
        )

        subscriptionService.subscribe(requireNotNull(subscriber.id), requireNotNull(author.id))
        // 의도적으로 read model 동기화를 기다리지 않는다 — 레이스를 그대로 재현하는 것이 이 테스트의 목적.
        val post = postService.create(requireNotNull(author.id), "title", "content")
        postService.publish(requireNotNull(post.id))

        val recipientIds = awaitNotificationRecipients(requireNotNull(post.id), expectedCount = 1)
        assertEquals(listOf(subscriber.id), recipientIds)
    }

    private fun awaitNotificationRecipients(postId: Long, expectedCount: Int): List<Long> {
        // 컨슈머 자체의 재시도 예산(findSubscribersWithRetry, 약 4초)보다 넉넉하게 잡는다 —
        // 전체 스위트를 함께 돌릴 때는 다른 테스트 클래스가 같은 Kafka consumer group을 방금
        // 막 떠난 직후라 리밸런싱 오버헤드가 추가로 끼어들 수 있다(테스트 인프라 특유의 잡음이지
        // 프로덕션 재시도 로직의 결함이 아니다).
        val deadline = System.currentTimeMillis() + 30_000
        while (System.currentTimeMillis() < deadline) {
            val ids = jdbcTemplate.queryForList(
                "SELECT recipient_id FROM notification.notifications WHERE post_id = :postId ORDER BY recipient_id",
                MapSqlParameterSource("postId", postId),
                Long::class.java,
            ).filterNotNull()
            if (ids.size >= expectedCount) return ids
            Thread.sleep(300)
        }
        throw AssertionError("Timed out waiting for notifications for post $postId")
    }
}
