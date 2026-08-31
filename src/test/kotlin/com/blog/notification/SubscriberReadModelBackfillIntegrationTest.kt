package com.blog.notification

import com.blog.notification.notification.backfill.SubscriberReadModelBackfillService
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.transaction.annotation.Transactional

@Import(TestcontainersConfiguration::class)
@SpringBootTest
@Transactional
class SubscriberReadModelBackfillIntegrationTest {

    @Autowired
    lateinit var backfillService: SubscriberReadModelBackfillService

    @Autowired
    lateinit var jdbcTemplate: NamedParameterJdbcTemplate

    @Test
    fun `backfill copies only active subscriptions with keyset pagination and is repeatable`() {
        jdbcTemplate.update("DELETE FROM notification.subscriber_read_model", MapSqlParameterSource())
        jdbcTemplate.update("DELETE FROM subscription.subscriptions", MapSqlParameterSource())

        insertSubscription(101, 201, "ACTIVE")
        insertSubscription(102, 201, "CANCELLED")
        insertSubscription(103, 202, "ACTIVE")
        insertSubscription(104, 202, "ACTIVE")
        insertReadModel(999, 999)

        val first = backfillService.backfill(pageSize = 2)

        assertEquals(3, first.upsertedRows)
        assertEquals(1, first.removedStaleRows)
        assertEquals(2, first.pages)
        assertEquals(
            setOf(201L to 101L, 202L to 103L, 202L to 104L),
            readModelRows(),
        )

        val second = backfillService.backfill(pageSize = 2)

        assertEquals(3, second.upsertedRows)
        assertEquals(0, second.removedStaleRows)
        assertEquals(2, second.pages)
        assertEquals(3, readModelRows().size)
    }

    private fun insertSubscription(userId: Long, authorId: Long, status: String) {
        jdbcTemplate.update(
            """
            INSERT INTO subscription.subscriptions
                (user_id, author_id, status, subscribed_at, cancelled_at)
            VALUES
                (:userId, :authorId, :status, now(),
                 CASE WHEN :status = 'CANCELLED' THEN now() ELSE NULL END)
            """.trimIndent(),
            MapSqlParameterSource("userId", userId)
                .addValue("authorId", authorId)
                .addValue("status", status),
        )
    }

    private fun insertReadModel(authorId: Long, userId: Long) {
        jdbcTemplate.update(
            """
            INSERT INTO notification.subscriber_read_model (author_id, user_id)
            VALUES (:authorId, :userId)
            """.trimIndent(),
            MapSqlParameterSource("authorId", authorId).addValue("userId", userId),
        )
    }

    private fun readModelRows(): Set<Pair<Long, Long>> =
        jdbcTemplate.query(
            "SELECT author_id, user_id FROM notification.subscriber_read_model",
            MapSqlParameterSource(),
        ) { rs, _ -> rs.getLong("author_id") to rs.getLong("user_id") }.toSet()
}
