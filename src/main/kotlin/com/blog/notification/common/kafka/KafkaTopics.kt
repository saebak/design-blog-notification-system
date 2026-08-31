package com.blog.notification.common.kafka

object KafkaTopics {
    const val POST_PUBLISHED = "post.published"
    const val SUBSCRIPTION_CHANGED = "subscription.changed"
    const val FANOUT_CHUNK_REQUESTED = "fanout.chunk.requested"
}
