package com.blog.notification.notification.repository

import com.blog.notification.subscription.Subscription
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository

@Repository
class SubscriberReadModelJdbcDao(
    private val jdbcTemplate: NamedParameterJdbcTemplate,
) {
    fun upsert(authorId: Long, userId: Long) {
        val sql = """
            INSERT INTO notification.subscriber_read_model (author_id, user_id, updated_at)
            VALUES (:authorId, :userId, now())
            ON CONFLICT (author_id, user_id) DO UPDATE SET updated_at = now()
        """.trimIndent()
        jdbcTemplate.update(sql, MapSqlParameterSource("authorId", authorId).addValue("userId", userId))
    }

    fun delete(authorId: Long, userId: Long) {
        val sql = """
            DELETE FROM notification.subscriber_read_model
            WHERE author_id = :authorId AND user_id = :userId
        """.trimIndent()
        jdbcTemplate.update(sql, MapSqlParameterSource("authorId", authorId).addValue("userId", userId))
    }

    fun upsertAll(subscriptions: List<Subscription>): Int {
        if (subscriptions.isEmpty()) return 0

        val sql = """
            INSERT INTO notification.subscriber_read_model (author_id, user_id, updated_at)
            VALUES (:authorId, :userId, now())
            ON CONFLICT (author_id, user_id) DO UPDATE SET updated_at = now()
        """.trimIndent()
        val params = subscriptions.map { subscription ->
            MapSqlParameterSource("authorId", subscription.authorId)
                .addValue("userId", subscription.userId)
        }.toTypedArray()
        return jdbcTemplate.batchUpdate(sql, params).sum()
    }

    /** Removes rows that can remain after rebuilding a previously populated read model. */
    fun deleteNotBackedByActiveSubscription(): Int {
        val sql = """
            DELETE FROM notification.subscriber_read_model read_model
            WHERE NOT EXISTS (
                SELECT 1
                FROM subscription.subscriptions source
                WHERE source.author_id = read_model.author_id
                  AND source.user_id = read_model.user_id
                  AND source.status = 'ACTIVE'
            )
        """.trimIndent()
        return jdbcTemplate.update(sql, MapSqlParameterSource())
    }

    fun findUserIdsByAuthor(authorId: Long): List<Long> {
        val sql = "SELECT user_id FROM notification.subscriber_read_model WHERE author_id = :authorId"
        return jdbcTemplate.queryForList(sql, MapSqlParameterSource("authorId", authorId), Long::class.java)
            .filterNotNull()
    }
}
