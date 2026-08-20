package com.blog.notification.post

import java.time.Instant
import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Table

@Table(schema = "post", value = "posts")
data class Post(
    @Id val id: Long? = null,
    val authorId: Long,
    val title: String,
    val content: String,
    val status: PostStatus = PostStatus.DRAFT,
    val publishedAt: Instant? = null,
    val createdAt: Instant = Instant.now(),
    val updatedAt: Instant = Instant.now(),
) {
    // 이미 Published면 그대로 반환 — 재발행 이벤트가 또 나가면 안 되니까
    fun publish(now: Instant = Instant.now()): Post =
        if (status == PostStatus.PUBLISHED) this
        else copy(status = PostStatus.PUBLISHED, publishedAt = now, updatedAt = now)
}
