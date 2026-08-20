package com.blog.notification.post.service

import com.blog.notification.common.NotFoundException
import com.blog.notification.post.Post
import com.blog.notification.post.PostPublishedEvent
import com.blog.notification.post.PostStatus
import com.blog.notification.post.repository.OutboxEventJdbcDao
import com.blog.notification.post.repository.PostRepository
import com.blog.notification.user.repository.UserRepository
import java.util.UUID
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import tools.jackson.databind.ObjectMapper

@Service
class PostService(
    private val postRepository: PostRepository,
    private val outboxEventDao: OutboxEventJdbcDao,
    private val userRepository: UserRepository,
    private val objectMapper: ObjectMapper,
) {
    @Transactional
    fun create(authorId: Long, title: String, content: String): Post {
        if (!userRepository.existsById(authorId)) {
            throw NotFoundException("User not found: $authorId")
        }
        return postRepository.save(Post(authorId = authorId, title = title, content = content))
    }

    fun getById(id: Long): Post =
        postRepository.findById(id).orElseThrow { NotFoundException("Post not found: $id") }

    // Post 저장과 outbox 적재를 한 트랜잭션으로 묶어서, 발행됐는데 이벤트가 안 쌓이는 일이 없게 한다.
    // 실제 브로커 발행은 Relay가 따로 폴링해서 처리 (아직 미구현).
    @Transactional
    fun publish(id: Long): Post {
        val post = getById(id)
        if (post.status == PostStatus.PUBLISHED) {
            return post
        }
        val published = post.publish()
        val saved = postRepository.save(published)

        val event = PostPublishedEvent(
            eventId = UUID.randomUUID(),
            postId = requireNotNull(saved.id),
            authorId = saved.authorId,
            title = saved.title,
            publishedAt = requireNotNull(saved.publishedAt),
        )
        outboxEventDao.insertPending(
            aggregateType = "Post",
            aggregateId = requireNotNull(saved.id),
            eventType = "PostPublished",
            payloadJson = objectMapper.writeValueAsString(event),
        )
        return saved
    }
}
