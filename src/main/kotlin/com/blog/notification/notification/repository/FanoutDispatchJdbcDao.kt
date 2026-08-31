package com.blog.notification.notification.repository

import java.util.UUID
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository

data class FanoutDispatchState(val authorId: Long, val cursorUserId: Long?, val done: Boolean)

@Repository
class FanoutDispatchJdbcDao(private val jdbcTemplate: NamedParameterJdbcTemplate) {
    fun find(eventId: UUID): FanoutDispatchState? = jdbcTemplate.query(
        "SELECT author_id, cursor_user_id, status FROM notification.fanout_dispatches WHERE event_id = :eventId",
        MapSqlParameterSource("eventId", eventId),
    ) { rs, _ -> FanoutDispatchState(rs.getLong("author_id"), rs.getObject("cursor_user_id")?.let { (it as Number).toLong() }, rs.getString("status") == "DONE") }
        .firstOrNull()

    fun start(eventId: UUID, authorId: Long) = jdbcTemplate.update(
        "INSERT INTO notification.fanout_dispatches (event_id, author_id, status) VALUES (:eventId, :authorId, 'IN_PROGRESS') ON CONFLICT (event_id) DO NOTHING",
        MapSqlParameterSource("eventId", eventId).addValue("authorId", authorId),
    )

    fun advance(eventId: UUID, cursorUserId: Long) = jdbcTemplate.update(
        "UPDATE notification.fanout_dispatches SET cursor_user_id = :cursorUserId, updated_at = now() WHERE event_id = :eventId AND status = 'IN_PROGRESS'",
        MapSqlParameterSource("eventId", eventId).addValue("cursorUserId", cursorUserId),
    )

    fun markDone(eventId: UUID) = jdbcTemplate.update(
        "UPDATE notification.fanout_dispatches SET status = 'DONE', updated_at = now() WHERE event_id = :eventId",
        MapSqlParameterSource("eventId", eventId),
    )
}
