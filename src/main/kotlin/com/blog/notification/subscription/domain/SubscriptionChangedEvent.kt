package com.blog.notification.subscription

import java.time.Instant
import java.util.UUID

data class SubscriptionChangedEvent(
    val eventId: UUID,
    val userId: Long,
    val authorId: Long,
    val status: SubscriptionStatus,
    val occurredAt: Instant,
)
