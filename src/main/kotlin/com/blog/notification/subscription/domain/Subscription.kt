package com.blog.notification.subscription

import java.time.Instant
import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Table

// userId(구독자)가 authorId(작가)를 구독하는 관계. 둘 다 그냥 User.id고 별도 Author 타입은 없다.
@Table(schema = "subscription", value = "subscriptions")
data class Subscription(
    @Id val id: Long? = null,
    val userId: Long,
    val authorId: Long,
    val status: SubscriptionStatus,
    val subscribedAt: Instant,
    val cancelledAt: Instant?,
)
