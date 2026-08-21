package com.blog.notification.notification.repository

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

    fun findUserIdsByAuthor(authorId: Long): List<Long> {
        val sql = "SELECT user_id FROM notification.subscriber_read_model WHERE author_id = :authorId"
        return jdbcTemplate.queryForList(sql, MapSqlParameterSource("authorId", authorId), Long::class.java)
            .filterNotNull()
    }
}
