package com.blog.notification.notification.consumer.dto

import java.time.Instant
import java.util.UUID

// Post Context의 PostPublishedEvent와 필드는 같지만, Notification Context는 그 클래스를 직접
// import하지 않는다 — Published Language(JSON 계약)만 공유하고 코드 결합은 만들지 않는다.
data class PostPublishedMessage(
    val eventId: UUID,
    val postId: Long,
    val authorId: Long,
    val title: String,
    val publishedAt: Instant,
)
