package com.blog.notification.common.kafka

import org.apache.kafka.clients.admin.NewTopic
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

// docs/architecture.md §7의 파티션 수 그대로 명시 생성한다. 브로커의 auto.create.topics.enable에
// 맡기면 기본 파티션 수(1)로 생성돼 subscription.changed의 컨슈머 병렬 소비(§1, docs/decisions.md)가
// 애초에 불가능해진다 — 이미 다른 파티션 수로 생성된 토픽이 남아있는 로컬 환경(볼륨 유지)에서는
// KafkaAdmin이 파티션 수를 늘려주지 않으므로, 토픽을 다시 만들려면 `docker compose down -v`가 필요하다.
@Configuration
class KafkaTopicConfig {

    @Bean
    fun postPublishedTopic(): NewTopic = TopicBuilder.name(KafkaTopics.POST_PUBLISHED).partitions(6).replicas(1).build()

    @Bean
    fun subscriptionChangedTopic(): NewTopic =
        TopicBuilder.name(KafkaTopics.SUBSCRIPTION_CHANGED).partitions(6).replicas(1).build()

    @Bean
    fun fanoutChunkRequestedTopic(): NewTopic =
        TopicBuilder.name(KafkaTopics.FANOUT_CHUNK_REQUESTED).partitions(32).replicas(1).build()
}
