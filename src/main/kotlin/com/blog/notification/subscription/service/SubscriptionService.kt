package com.blog.notification.subscription.service

import com.blog.notification.common.NotFoundException
import com.blog.notification.subscription.Subscription
import com.blog.notification.subscription.repository.SubscriptionJdbcDao
import com.blog.notification.user.repository.UserRepository
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class SubscriptionService(
    private val subscriptionDao: SubscriptionJdbcDao,
    private val userRepository: UserRepository,
) {
    @Transactional
    fun subscribe(userId: Long, authorId: Long): Subscription {
        require(userId != authorId) { "A user cannot subscribe to themselves" }
        requireUserExists(userId)
        requireUserExists(authorId)
        return subscriptionDao.upsertActive(userId, authorId)
    }

    @Transactional
    fun cancel(userId: Long, authorId: Long): Subscription {
        return subscriptionDao.cancel(userId, authorId)
            ?: throw NotFoundException("Subscription not found: userId=$userId, authorId=$authorId")
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
