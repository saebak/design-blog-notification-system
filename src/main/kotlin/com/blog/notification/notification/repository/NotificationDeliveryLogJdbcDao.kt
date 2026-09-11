package com.blog.notification.notification.repository

import java.util.UUID
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
    val claimToken: UUID,
)

@Repository
class NotificationDeliveryLogJdbcDao(
    private val jdbcTemplate: NamedParameterJdbcTemplate,
) {
    fun countByStatus(status: String): Long = jdbcTemplate.queryForObject(
        "SELECT count(*) FROM notification.notification_delivery_log WHERE status = :status",
        MapSqlParameterSource("status", status),
        Long::class.java,
    ) ?: 0L

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
    fun claimDue(limit: Int, maxAttempts: Int, leaseSeconds: Long): List<DeliveryAttemptRecord> {
        val claimToken = UUID.randomUUID()
        val sql = """
            WITH candidates AS (
                SELECT dl.id
                FROM notification.notification_delivery_log dl
                WHERE dl.channel = 'PUSH'
                  AND dl.attempt_count < :maxAttempts
                  AND (
                      (
                          dl.status IN ('PENDING', 'FAILED')
                          AND (
                              dl.attempt_count = 0
                              OR dl.last_attempt_at <= now() - (power(2, dl.attempt_count - 1) * interval '1 second')
                          )
                      )
                      OR (dl.status = 'PROCESSING' AND dl.claimed_until <= now())
                  )
                ORDER BY dl.created_at
                FOR UPDATE SKIP LOCKED
                LIMIT :limit
            )
            UPDATE notification.notification_delivery_log dl
            SET status = 'PROCESSING', claim_token = :claimToken,
                claimed_until = now() + (:leaseSeconds * interval '1 second')
            FROM candidates, notification.notifications n
            WHERE dl.id = candidates.id AND n.id = dl.notification_id
            RETURNING dl.id, dl.notification_id, n.recipient_id, n.title, dl.attempt_count, dl.claim_token
        """.trimIndent()
        val params = MapSqlParameterSource()
            .addValue("limit", limit)
            .addValue("maxAttempts", maxAttempts)
            .addValue("claimToken", claimToken)
            .addValue("leaseSeconds", leaseSeconds)
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
                    claimToken = UUID.fromString(rs.getString("claim_token")),
                )
            },
        )
    }

    fun markSent(id: Long, claimToken: UUID): Boolean {
        val sql = """
            UPDATE notification.notification_delivery_log
            SET status = 'SENT', attempt_count = attempt_count + 1, last_attempt_at = now(),
                claim_token = NULL, claimed_until = NULL
            WHERE id = :id AND status = 'PROCESSING' AND claim_token = :claimToken
        """.trimIndent()
        return jdbcTemplate.update(sql, MapSqlParameterSource("id", id).addValue("claimToken", claimToken)) == 1
    }

    fun recordFailedAttempt(id: Long, claimToken: UUID, maxAttempts: Int): String? {
        val sql = """
            UPDATE notification.notification_delivery_log
            SET attempt_count = attempt_count + 1,
                last_attempt_at = now(),
                status = CASE WHEN attempt_count + 1 >= :maxAttempts THEN 'DEAD_LETTER' ELSE 'FAILED' END,
                claim_token = NULL,
                claimed_until = NULL
            WHERE id = :id AND status = 'PROCESSING' AND claim_token = :claimToken
        """.trimIndent()
        val params = MapSqlParameterSource()
            .addValue("id", id)
            .addValue("claimToken", claimToken)
            .addValue("maxAttempts", maxAttempts)
        return jdbcTemplate.queryForList(
            "$sql RETURNING status",
            params,
            String::class.java,
        ).firstOrNull()
    }

    fun requeueDeadLetter(id: Long): Boolean = jdbcTemplate.update(
        """
        UPDATE notification.notification_delivery_log
        SET status = 'PENDING', attempt_count = 0, last_attempt_at = NULL,
            claim_token = NULL, claimed_until = NULL
        WHERE id = :id AND status = 'DEAD_LETTER'
        """.trimIndent(),
        MapSqlParameterSource("id", id),
    ) == 1
}
