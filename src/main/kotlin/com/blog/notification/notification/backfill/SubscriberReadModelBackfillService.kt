package com.blog.notification.notification.backfill

import com.blog.notification.notification.repository.SubscriberReadModelJdbcDao
import com.blog.notification.subscription.repository.SubscriptionJdbcDao
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.support.TransactionTemplate

data class SubscriberReadModelBackfillResult(
    val upsertedRows: Int,
    val removedStaleRows: Int,
    val pages: Int,
)

@Service
class SubscriberReadModelBackfillService(
    private val subscriptionDao: SubscriptionJdbcDao,
    private val subscriberReadModelDao: SubscriberReadModelJdbcDao,
    private val transactionTemplate: TransactionTemplate,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun backfill(pageSize: Int): SubscriberReadModelBackfillResult {
        require(pageSize > 0) { "pageSize must be greater than zero" }

        val removedStaleRows = transactionTemplate.execute {
            subscriberReadModelDao.deleteNotBackedByActiveSubscription()
        }

        var cursor: Long? = null
        var upsertedRows = 0
        var pages = 0

        while (true) {
            val page = transactionTemplate.execute {
                val subscriptions = subscriptionDao.findActiveAfterIdForBackfill(cursor, pageSize)
                subscriberReadModelDao.upsertAll(subscriptions)
                subscriptions
            }.orEmpty()

            if (page.isEmpty()) break

            cursor = requireNotNull(page.last().id)
            upsertedRows += page.size
            pages++
            log.info("Subscriber read model backfill page completed: page={}, cursor={}, rows={}", pages, cursor, page.size)

            if (page.size < pageSize) break
        }

        return SubscriberReadModelBackfillResult(upsertedRows, removedStaleRows, pages).also {
            log.info("Subscriber read model backfill completed: {}", it)
        }
    }
}
