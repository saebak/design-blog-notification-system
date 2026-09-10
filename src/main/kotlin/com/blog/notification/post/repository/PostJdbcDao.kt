package com.blog.notification.post.repository

import java.time.Instant
import java.sql.Timestamp
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository

@Repository
class PostJdbcDao(private val jdbcTemplate: NamedParameterJdbcTemplate) {

    /**
     * Atomically elects the single request that publishes a draft.
     * Concurrent callers that arrive after the winning update receive 0 and must not emit another event.
     */
    fun publishIfDraft(id: Long, publishedAt: Instant): Boolean = jdbcTemplate.update(
        """
        UPDATE post.posts
        SET status = 'PUBLISHED', published_at = :publishedAt, updated_at = :publishedAt
        WHERE id = :id AND status = 'DRAFT'
        """.trimIndent(),
        MapSqlParameterSource("id", id).addValue("publishedAt", Timestamp.from(publishedAt)),
    ) == 1
}
