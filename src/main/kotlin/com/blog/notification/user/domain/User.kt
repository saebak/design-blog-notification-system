package com.blog.notification.user

import java.time.Instant
import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Table

// 작가/구독자 구분 없이 전부 User다. Post.authorId, Subscription.userId/authorId는
// 그냥 이 id 값을 참조만 할 뿐 FK는 없다.
@Table(schema = "public", value = "users")
data class User(
    @Id val id: Long? = null,
    val email: String,
    val name: String,
    val notificationChannel: NotificationChannel = NotificationChannel.PUSH,
    val createdAt: Instant = Instant.now(),
)
