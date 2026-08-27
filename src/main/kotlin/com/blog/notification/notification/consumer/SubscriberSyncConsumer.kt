package com.blog.notification.notification.consumer

import com.blog.notification.common.kafka.KafkaTopics
import com.blog.notification.notification.consumer.dto.SubscriptionChangedMessage
import com.blog.notification.notification.repository.SubscriberReadModelJdbcDao
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import tools.jackson.databind.ObjectMapper

// subscription.changed를 소비해 Subscriber Read Model을 최신 상태로 유지한다.
// 배치 리스너로 폴링 단위(최대 500건, application.yml max.poll.records)를 한 번에 받아 처리한다 —
// 메시지마다 개별 커밋하던 것을 배치당 1커밋으로 묶어 대량 백로그 드레인 속도를 개선한다
// (docs/decisions.md §1, 실측: 259초 소요). 배치 안에서는 원래 메시지 순서 그대로 순회해서
// 같은 (authorId, userId)에 대한 ACTIVE→CANCELLED 같은 연쇄가 뒤바뀌지 않게 한다. concurrency는
// subscription.changed 파티션 수(6, KafkaTopicConfig)만큼 병렬 소비 — authorId 파티션 키 덕분에
// 같은 (authorId, userId) 쌍은 항상 같은 파티션에 들어와 순서가 깨지지 않는다(architecture.md §3).
// groupId를 PostPublishedFanoutConsumer와 분리한다(docs/decisions.md §5-b) — 같은 그룹을
// 공유한 채로 concurrency를 6으로 올리면, 서로 다른 구독(subscription.changed vs post.published)을
// 가진 멤버들이 한 그룹 안에서 계속 리밸런싱을 반복해 subscription.changed 쪽에 파티션이
// 아예 할당되지 않는 문제를 실제로 겪었다.
@Component
class SubscriberSyncConsumer(
    private val subscriberReadModelDao: SubscriberReadModelJdbcDao,
    private val objectMapper: ObjectMapper,
) {
    @KafkaListener(
        topics = [KafkaTopics.SUBSCRIPTION_CHANGED],
        groupId = "subscriber-sync",
        containerFactory = "batchListenerContainerFactory",
        concurrency = "6",
    )
    @Transactional
    fun onMessage(payloads: List<String>) {
        payloads.forEach { payload ->
            val message = objectMapper.readValue(payload, SubscriptionChangedMessage::class.java)
            when (message.status) {
                "ACTIVE" -> subscriberReadModelDao.upsert(message.authorId, message.userId)
                "CANCELLED" -> subscriberReadModelDao.delete(message.authorId, message.userId)
            }
        }
    }
}
