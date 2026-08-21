package com.blog.notification.notification.repository

import com.blog.notification.notification.Notification
import java.util.UUID
import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository

data class NotificationInsert(
    val recipientId: Long,
    val sourceEventId: UUID,
    val postId: Long,
    val authorId: Long,
    val title: String,
)

@Repository
class NotificationJdbcDao(
    private val jdbcTemplate: NamedParameterJdbcTemplate,
) {
    private val rowMapper = RowMapper { rs, _ ->
        Notification(
            id = rs.getLong("id"),
            recipientId = rs.getLong("recipient_id"),
            sourceEventId = UUID.fromString(rs.getString("source_event_id")),
            postId = rs.getLong("post_id"),
            authorId = rs.getLong("author_id"),
            title = rs.getString("title"),
            isRead = rs.getBoolean("is_read"),
            createdAt = rs.getTimestamp("created_at").toInstant(),
            readAt = rs.getTimestamp("read_at")?.toInstant(),
        )
    }

    // (recipient_id, source_event_id) UNIQUE 제약이 멱등성 최종 방어선.
    // DO UPDATE의 no-op 트릭으로 신규/기존 여부와 무관하게 항상 id를 돌려받는다 —
    // delivery log를 멱등하게 다시 확인하려면 재처리 시에도 notification id가 필요하다.
    fun insertAndGetId(notification: NotificationInsert): Long {
        val sql = """
            INSERT INTO notification.notifications (recipient_id, source_event_id, post_id, author_id, title)
            VALUES (:recipientId, :sourceEventId, :postId, :authorId, :title)
            ON CONFLICT (recipient_id, source_event_id)
            DO UPDATE SET recipient_id = notification.notifications.recipient_id
            RETURNING id
        """.trimIndent()
        val params = MapSqlParameterSource()
            .addValue("recipientId", notification.recipientId)
            .addValue("sourceEventId", notification.sourceEventId)
            .addValue("postId", notification.postId)
            .addValue("authorId", notification.authorId)
            .addValue("title", notification.title)
        return jdbcTemplate.queryForObject(sql, params, Long::class.java)!!
    }

    // recipientId 조건을 쿼리 자체에 강제해 타 사용자 알림을 읽음 처리할 수 없게 한다
    // (architecture.md §8.2 IDOR 방지 원칙 — 인증은 없지만 최소한 이 조건은 항상 건다).
    fun markRead(id: Long, recipientId: Long): Notification? {
        val sql = """
            UPDATE notification.notifications
            SET is_read = true, read_at = now()
            WHERE id = :id AND recipient_id = :recipientId
            RETURNING id, recipient_id, source_event_id, post_id, author_id, title, is_read, created_at, read_at
        """.trimIndent()
        val params = MapSqlParameterSource().addValue("id", id).addValue("recipientId", recipientId)
        val rows = jdbcTemplate.query(sql, params, rowMapper)
        return rows.firstOrNull()
    }

    // 이미 읽은 알림(is_read=true)은 WHERE 절에서 자연히 제외돼 갱신 대상이 아니다 —
    // 동시에 여러 번 호출돼도 두 번째 호출부터는 갱신 0건으로 수렴한다(동시성 안전).
    fun markAllRead(recipientId: Long): Int {
        val sql = """
            UPDATE notification.notifications
            SET is_read = true, read_at = now()
            WHERE recipient_id = :recipientId AND is_read = false
        """.trimIndent()
        return jdbcTemplate.update(sql, MapSqlParameterSource("recipientId", recipientId))
    }
}
