package com.blog.notification.notification.consumer.dto

import java.time.Instant
import java.util.UUID

// PostPublishedMessage와 같은 이유로, Subscription Context의 도메인 클래스를 직접 참조하지 않는다.
data class SubscriptionChangedMessage(
    val eventId: UUID,
    val userId: Long,
    val authorId: Long,
    val status: String,
    val occurredAt: Instant,
)
