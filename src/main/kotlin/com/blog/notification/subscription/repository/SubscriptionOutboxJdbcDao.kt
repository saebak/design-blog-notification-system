package com.blog.notification.subscription.repository

import com.blog.notification.common.outbox.OutboxEventRecord
import java.util.UUID
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository

// post.repository.OutboxEventJdbcDao와 동일 패턴, subscription 스키마 전용.
@Repository
class SubscriptionOutboxJdbcDao(
    private val jdbcTemplate: NamedParameterJdbcTemplate,
) {
    fun insertPending(aggregateId: Long, eventType: String, payloadJson: String) {
        val sql = """
            INSERT INTO subscription.subscription_outbox_events (aggregate_type, aggregate_id, event_type, payload, status)
            VALUES ('Subscription', :aggregateId, :eventType, CAST(:payload AS JSONB), 'PENDING')
        """.trimIndent()
        val params = MapSqlParameterSource()
            .addValue("aggregateId", aggregateId)
            .addValue("eventType", eventType)
            .addValue("payload", payloadJson)
        jdbcTemplate.update(sql, params)
    }

    fun findPending(limit: Int): List<OutboxEventRecord> {
        val sql = """
            SELECT id, event_type, payload
            FROM subscription.subscription_outbox_events
            WHERE status = 'PENDING'
            ORDER BY created_at
            LIMIT :limit
        """.trimIndent()
        return jdbcTemplate.query(sql, MapSqlParameterSource("limit", limit)) { rs, _ ->
            OutboxEventRecord(
                id = UUID.fromString(rs.getString("id")),
                eventType = rs.getString("event_type"),
                payload = rs.getString("payload"),
            )
        }
    }

    fun markPublished(id: UUID) {
        val sql = """
            UPDATE subscription.subscription_outbox_events
            SET status = 'PUBLISHED', published_at = now()
            WHERE id = :id
        """.trimIndent()
        jdbcTemplate.update(sql, MapSqlParameterSource("id", id))
    }
}
