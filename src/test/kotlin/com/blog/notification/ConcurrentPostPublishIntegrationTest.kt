package com.blog.notification

import com.blog.notification.post.PostStatus
import com.blog.notification.post.service.PostService
import com.blog.notification.user.User
import com.blog.notification.user.repository.UserRepository
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate

@Import(TestcontainersConfiguration::class)
@SpringBootTest
class ConcurrentPostPublishIntegrationTest {

    @Autowired lateinit var userRepository: UserRepository
    @Autowired lateinit var postService: PostService
    @Autowired lateinit var jdbcTemplate: NamedParameterJdbcTemplate

    @Test
    fun `concurrent publish creates one logical event`() {
        val author = userRepository.save(User(email = "concurrent-publish-author@test.com", name = "author"))
        val post = postService.create(requireNotNull(author.id), "concurrent publish", "content")
        val postId = requireNotNull(post.id)
        val ready = CountDownLatch(2)
        val start = CountDownLatch(1)
        val executor = Executors.newFixedThreadPool(2)

        try {
            val calls = (1..2).map {
                executor.submit(Callable {
                    ready.countDown()
                    start.await()
                    postService.publish(postId)
                })
            }
            ready.await()
            start.countDown()
            calls.forEach { assertEquals(PostStatus.PUBLISHED, it.get().status) }
        } finally {
            executor.shutdownNow()
        }

        val eventCount = jdbcTemplate.queryForObject(
            """
            SELECT count(*) FROM post.outbox_events
            WHERE aggregate_type = 'Post' AND aggregate_id = :postId AND event_type = 'PostPublished'
            """.trimIndent(),
            MapSqlParameterSource("postId", postId),
            Long::class.java,
        )
        assertEquals(1L, eventCount)
    }
}
