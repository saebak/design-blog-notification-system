package com.blog.notification.post.repository

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository

// payload가 JSONB라 Spring Data JDBC 매핑 대신 직접 SQL로 처리.
// 브로커에 실제로 발행하는 Relay는 아직 없고, 여기선 PENDING으로 쌓기만 한다.
@Repository
class OutboxEventJdbcDao(
    private val jdbcTemplate: NamedParameterJdbcTemplate,
) {
    fun insertPending(aggregateType: String, aggregateId: Long, eventType: String, payloadJson: String) {
        val sql = """
            INSERT INTO post.outbox_events (aggregate_type, aggregate_id, event_type, payload, status)
            VALUES (:aggregateType, :aggregateId, :eventType, CAST(:payload AS JSONB), 'PENDING')
        """.trimIndent()
        val params = MapSqlParameterSource()
            .addValue("aggregateType", aggregateType)
            .addValue("aggregateId", aggregateId)
            .addValue("eventType", eventType)
            .addValue("payload", payloadJson)
        jdbcTemplate.update(sql, params)
    }
}
