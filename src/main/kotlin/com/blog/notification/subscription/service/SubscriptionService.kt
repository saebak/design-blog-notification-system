package com.blog.notification.subscription.service

import com.blog.notification.common.NotFoundException
import com.blog.notification.subscription.Subscription
import com.blog.notification.subscription.SubscriptionChangedEvent
import com.blog.notification.subscription.repository.SubscriptionJdbcDao
import com.blog.notification.subscription.repository.SubscriptionOutboxJdbcDao
import com.blog.notification.user.repository.UserRepository
import java.time.Instant
import java.util.UUID
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import tools.jackson.databind.ObjectMapper

@Service
class SubscriptionService(
    private val subscriptionDao: SubscriptionJdbcDao,
    private val outboxDao: SubscriptionOutboxJdbcDao,
    private val userRepository: UserRepository,
    private val objectMapper: ObjectMapper,
) {
    // Subscription 저장과 outbox 적재를 한 트랜잭션으로 묶는다 (Post와 동일 패턴).
    @Transactional
    fun subscribe(userId: Long, authorId: Long): Subscription {
        require(userId != authorId) { "A user cannot subscribe to themselves" }
        requireUserExists(userId)
        requireUserExists(authorId)
        val subscription = subscriptionDao.upsertActive(userId, authorId)
        publishChanged(subscription)
        return subscription
    }

    @Transactional
    fun cancel(userId: Long, authorId: Long): Subscription {
        val subscription = subscriptionDao.cancel(userId, authorId)
            ?: throw NotFoundException("Subscription not found: userId=$userId, authorId=$authorId")
        publishChanged(subscription)
        return subscription
    }

    // 이미 같은 상태였던 경우(no-op)에도 이벤트를 낸다 — 컨슈머가 last-write-wins upsert/delete로
    // 처리하므로 중복 발행은 무해하다.
    private fun publishChanged(subscription: Subscription) {
        val event = SubscriptionChangedEvent(
            eventId = UUID.randomUUID(),
            userId = subscription.userId,
            authorId = subscription.authorId,
            status = subscription.status,
            occurredAt = Instant.now(),
        )
        outboxDao.insertPending(
            aggregateId = requireNotNull(subscription.id),
            eventType = "SubscriptionChanged",
            payloadJson = objectMapper.writeValueAsString(event),
        )
    }

    fun listMySubscriptions(userId: Long, cursor: Long?, limit: Int): List<Subscription> =
        subscriptionDao.findActiveByUser(userId, cursor, limit)

    fun listSubscribers(authorId: Long, cursor: Long?, limit: Int): List<Subscription> =
        subscriptionDao.findActiveByAuthor(authorId, cursor, limit)

    private fun requireUserExists(userId: Long) {
        if (!userRepository.existsById(userId)) {
            throw NotFoundException("User not found: $userId")
        }
    }
}
