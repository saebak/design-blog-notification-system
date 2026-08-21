package com.blog.notification.notification.repository

import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository

data class DeliveryAttemptRecord(
    val id: Long,
    val notificationId: Long,
    val recipientId: Long,
    val title: String,
    val attemptCount: Int,
)

@Repository
class NotificationDeliveryLogJdbcDao(
    private val jdbcTemplate: NamedParameterJdbcTemplate,
) {
    fun insertPending(notificationId: Long, channel: String = "PUSH") {
        val sql = """
            INSERT INTO notification.notification_delivery_log (notification_id, channel, status)
            VALUES (:notificationId, :channel, 'PENDING')
            ON CONFLICT (notification_id, channel) DO NOTHING
        """.trimIndent()
        val params = MapSqlParameterSource()
            .addValue("notificationId", notificationId)
            .addValue("channel", channel)
        jdbcTemplate.update(sql, params)
    }

    // attempt 1회차(attempt_count=0)는 즉시, 이후엔 2^(attempt_count-1)초 백오프(1s, 2s, ...).
    fun findDue(limit: Int, maxAttempts: Int): List<DeliveryAttemptRecord> {
        val sql = """
            SELECT dl.id, dl.notification_id, n.recipient_id, n.title, dl.attempt_count
            FROM notification.notification_delivery_log dl
            JOIN notification.notifications n ON n.id = dl.notification_id
            WHERE dl.channel = 'PUSH'
              AND dl.status IN ('PENDING', 'FAILED')
              AND dl.attempt_count < :maxAttempts
              AND (
                  dl.attempt_count = 0
                  OR dl.last_attempt_at <= now() - (power(2, dl.attempt_count - 1) * interval '1 second')
              )
            ORDER BY dl.created_at
            LIMIT :limit
        """.trimIndent()
        val params = MapSqlParameterSource()
            .addValue("limit", limit)
            .addValue("maxAttempts", maxAttempts)
        return jdbcTemplate.query(
            sql,
            params,
            RowMapper { rs, _ ->
                DeliveryAttemptRecord(
                    id = rs.getLong("id"),
                    notificationId = rs.getLong("notification_id"),
                    recipientId = rs.getLong("recipient_id"),
                    title = rs.getString("title"),
                    attemptCount = rs.getInt("attempt_count"),
                )
            },
        )
    }

    fun markSent(id: Long) {
        val sql = """
            UPDATE notification.notification_delivery_log
            SET status = 'SENT', attempt_count = attempt_count + 1, last_attempt_at = now()
            WHERE id = :id
        """.trimIndent()
        jdbcTemplate.update(sql, MapSqlParameterSource("id", id))
    }

    fun recordFailedAttempt(id: Long, maxAttempts: Int) {
        val sql = """
            UPDATE notification.notification_delivery_log
            SET attempt_count = attempt_count + 1,
                last_attempt_at = now(),
                status = CASE WHEN attempt_count + 1 >= :maxAttempts THEN 'DEAD_LETTER' ELSE 'FAILED' END
            WHERE id = :id
        """.trimIndent()
        val params = MapSqlParameterSource()
            .addValue("id", id)
            .addValue("maxAttempts", maxAttempts)
        jdbcTemplate.update(sql, params)
    }
}
