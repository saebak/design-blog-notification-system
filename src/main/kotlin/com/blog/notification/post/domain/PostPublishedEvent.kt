package com.blog.notification.post

import java.time.Instant
import java.util.UUID

// eventId는 나중에 Fan-out 쪽 멱등성 키로 쓰인다.
data class PostPublishedEvent(
    val eventId: UUID,
    val postId: Long,
    val authorId: Long,
    val title: String,
    val publishedAt: Instant,
)
