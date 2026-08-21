package com.blog.notification.common.outbox

import java.util.UUID

data class OutboxEventRecord(
    val id: UUID,
    val eventType: String,
    val payload: String,
)
