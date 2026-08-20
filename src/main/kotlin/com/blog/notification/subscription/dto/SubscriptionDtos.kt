package com.blog.notification.subscription.dto

import com.blog.notification.subscription.Subscription
import com.blog.notification.subscription.SubscriptionStatus
import jakarta.validation.constraints.NotNull
import java.time.Instant

data class SubscribeRequest(
    @field:NotNull val userId: Long,
    @field:NotNull val authorId: Long,
)

data class SubscriptionResponse(
    val id: Long,
    val userId: Long,
    val authorId: Long,
    val status: SubscriptionStatus,
    val subscribedAt: Instant,
    val cancelledAt: Instant?,
) {
    companion object {
        fun from(subscription: Subscription) = SubscriptionResponse(
            id = requireNotNull(subscription.id),
            userId = subscription.userId,
            authorId = subscription.authorId,
            status = subscription.status,
            subscribedAt = subscription.subscribedAt,
            cancelledAt = subscription.cancelledAt,
        )
    }
}
