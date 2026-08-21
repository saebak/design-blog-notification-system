package com.blog.notification.notification.dto

import com.blog.notification.notification.Notification
import java.time.Instant

data class NotificationResponse(
    val id: Long,
    val recipientId: Long,
    val postId: Long,
    val authorId: Long,
    val title: String,
    val isRead: Boolean,
    val createdAt: Instant,
    val readAt: Instant?,
) {
    companion object {
        fun from(notification: Notification) = NotificationResponse(
            id = notification.id,
            recipientId = notification.recipientId,
            postId = notification.postId,
            authorId = notification.authorId,
            title = notification.title,
            isRead = notification.isRead,
            createdAt = notification.createdAt,
            readAt = notification.readAt,
        )
    }
}

data class MarkAllReadResponse(
    val updatedCount: Int,
)
