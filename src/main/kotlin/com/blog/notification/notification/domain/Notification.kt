package com.blog.notification.notification

import java.time.Instant
import java.util.UUID

// Spring Data JDBC 엔티티로 두지 않고 NotificationJdbcDao의 RowMapper 결과로만 쓴다 —
// insertAndGetId가 upsert(ON CONFLICT ... DO UPDATE ... RETURNING)라 save()로 표현 불가
// (subscription.repository.SubscriptionJdbcDao와 동일한 이유).
data class Notification(
    val id: Long,
    val recipientId: Long,
    val sourceEventId: UUID,
    val postId: Long,
    val authorId: Long,
    val title: String,
    val isRead: Boolean,
    val createdAt: Instant,
    val readAt: Instant?,
)
