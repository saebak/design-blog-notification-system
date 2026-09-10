package com.blog.notification.notification.repository

import java.time.Instant
import java.util.UUID
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository

data class FanoutDispatchState(
    val eventId: UUID,
    val postId: Long,
    val authorId: Long,
    val title: String,
    val cursorUserId: Long?,
    val status: String,
    val retryCount: Int,
    val nextChunkIndex: Int,
    val nextRetryAt: Instant?,
)

@Repository
class FanoutDispatchJdbcDao(private val jdbcTemplate: NamedParameterJdbcTemplate) {
    fun countByStatus(status: String): Long = jdbcTemplate.queryForObject(
        "SELECT count(*) FROM notification.fanout_dispatches WHERE status = :status",
        MapSqlParameterSource("status", status),
        Long::class.java,
    ) ?: 0L

    fun find(eventId: UUID): FanoutDispatchState? = jdbcTemplate.query(
        """
        SELECT event_id, post_id, author_id, title, cursor_user_id, status,
               retry_count, next_chunk_index, next_retry_at
        FROM notification.fanout_dispatches
        WHERE event_id = :eventId
        """.trimIndent(),
        MapSqlParameterSource("eventId", eventId),
    ) { rs, _ ->
        FanoutDispatchState(
            eventId = UUID.fromString(rs.getString("event_id")),
            postId = rs.getLong("post_id"),
            authorId = rs.getLong("author_id"),
            title = rs.getString("title"),
            cursorUserId = rs.getObject("cursor_user_id")?.let { (it as Number).toLong() },
            status = rs.getString("status"),
            retryCount = rs.getInt("retry_count"),
            nextChunkIndex = rs.getInt("next_chunk_index"),
            nextRetryAt = rs.getTimestamp("next_retry_at")?.toInstant(),
        )
    }
        .firstOrNull()

    fun start(eventId: UUID, postId: Long, authorId: Long, title: String) = jdbcTemplate.update(
        """
        INSERT INTO notification.fanout_dispatches (event_id, post_id, author_id, title, status)
        VALUES (:eventId, :postId, :authorId, :title, 'IN_PROGRESS')
        ON CONFLICT (event_id) DO NOTHING
        """.trimIndent(),
        MapSqlParameterSource("eventId", eventId)
            .addValue("postId", postId)
            .addValue("authorId", authorId)
            .addValue("title", title),
    )

    fun findDueRetries(limit: Int): List<UUID> = jdbcTemplate.queryForList(
        """
        SELECT event_id
        FROM notification.fanout_dispatches
        WHERE status = 'WAITING_RETRY' AND next_retry_at <= now()
        ORDER BY next_retry_at
        LIMIT :limit
        """.trimIndent(),
        MapSqlParameterSource("limit", limit),
        UUID::class.java,
    ).filterNotNull()

    fun claimRetry(eventId: UUID): Boolean = jdbcTemplate.update(
        """
        UPDATE notification.fanout_dispatches
        SET status = 'IN_PROGRESS', next_retry_at = NULL, updated_at = now()
        WHERE event_id = :eventId
          AND status = 'WAITING_RETRY'
          AND next_retry_at <= now()
        """.trimIndent(),
        MapSqlParameterSource("eventId", eventId),
    ) == 1

    fun requeueFailed(eventId: UUID): Boolean = jdbcTemplate.update(
        """
        UPDATE notification.fanout_dispatches
        SET status = 'WAITING_RETRY', retry_count = 0, next_retry_at = now(),
            last_error = NULL, updated_at = now()
        WHERE event_id = :eventId AND status = 'FAILED'
        """.trimIndent(),
        MapSqlParameterSource("eventId", eventId),
    ) == 1

    fun advance(eventId: UUID, cursorUserId: Long) = jdbcTemplate.update(
        """
        UPDATE notification.fanout_dispatches
        SET cursor_user_id = :cursorUserId, next_chunk_index = next_chunk_index + 1, updated_at = now()
        WHERE event_id = :eventId AND status = 'IN_PROGRESS'
        """.trimIndent(),
        MapSqlParameterSource("eventId", eventId).addValue("cursorUserId", cursorUserId),
    )

    fun markDone(eventId: UUID) = jdbcTemplate.update(
        """
        UPDATE notification.fanout_dispatches
        SET status = 'DONE', next_retry_at = NULL, last_error = NULL, updated_at = now()
        WHERE event_id = :eventId
        """.trimIndent(),
        MapSqlParameterSource("eventId", eventId),
    )

    fun scheduleRetry(eventId: UUID, delaySeconds: Long, maxRetries: Int, reason: String): String {
        val nextStatus = if ((find(eventId)?.retryCount ?: 0) + 1 >= maxRetries) "FAILED" else "WAITING_RETRY"
        jdbcTemplate.update(
            """
            UPDATE notification.fanout_dispatches
            SET status = :status,
                retry_count = retry_count + 1,
                next_retry_at = CASE
                    WHEN :status = 'WAITING_RETRY' THEN now() + (:delaySeconds * interval '1 second')
                    ELSE NULL
                END,
                last_error = :reason,
                updated_at = now()
            WHERE event_id = :eventId AND status = 'IN_PROGRESS'
            """.trimIndent(),
            MapSqlParameterSource("eventId", eventId)
                .addValue("status", nextStatus)
                .addValue("delaySeconds", delaySeconds)
                .addValue("reason", reason),
        )
        return nextStatus
    }
}
