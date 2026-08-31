package com.blog.notification.notification.consumer.dto

import java.util.UUID

data class FanoutChunkRequestedMessage(
    val eventId: UUID,
    val postId: Long,
    val authorId: Long,
    val title: String,
    val chunkIndex: Int,
    val userIds: List<Long>,
)
