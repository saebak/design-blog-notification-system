package com.blog.notification.subscription.repository

import com.blog.notification.subscription.Subscription
import com.blog.notification.subscription.SubscriptionStatus
import java.sql.ResultSet
import org.springframework.dao.EmptyResultDataAccessException
import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository

// save()로는 upsert(ON CONFLICT ... DO UPDATE ... WHERE)를 표현할 수 없어서 직접 SQL로 처리한다.
@Repository
class SubscriptionJdbcDao(
    private val jdbcTemplate: NamedParameterJdbcTemplate,
) {
    private val rowMapper = RowMapper { rs: ResultSet, _: Int ->
        Subscription(
            id = rs.getLong("id"),
            userId = rs.getLong("user_id"),
            authorId = rs.getLong("author_id"),
            status = SubscriptionStatus.valueOf(rs.getString("status")),
            subscribedAt = rs.getTimestamp("subscribed_at").toInstant(),
            cancelledAt = rs.getTimestamp("cancelled_at")?.toInstant(),
        )
    }

    fun findByUserAndAuthor(userId: Long, authorId: Long): Subscription? {
        val sql = """
            SELECT id, user_id, author_id, status, subscribed_at, cancelled_at
            FROM subscription.subscriptions
            WHERE user_id = :userId AND author_id = :authorId
        """.trimIndent()
        val params = MapSqlParameterSource("userId", userId).addValue("authorId", authorId)
        return try {
            jdbcTemplate.queryForObject(sql, params, rowMapper)
        } catch (e: EmptyResultDataAccessException) {
            null
        }
    }

    // 이미 Active면 그대로 두고(멱등), Cancelled였다면 재활성화, 없으면 새로 만든다.
    fun upsertActive(userId: Long, authorId: Long): Subscription {
        val sql = """
            INSERT INTO subscription.subscriptions (user_id, author_id, status, subscribed_at, cancelled_at)
            VALUES (:userId, :authorId, 'ACTIVE', now(), NULL)
            ON CONFLICT (user_id, author_id)
            DO UPDATE SET status = 'ACTIVE', subscribed_at = now(), cancelled_at = NULL
            WHERE subscription.subscriptions.status = 'CANCELLED'
            RETURNING id, user_id, author_id, status, subscribed_at, cancelled_at
        """.trimIndent()
        val params = MapSqlParameterSource("userId", userId).addValue("authorId", authorId)
        return try {
            jdbcTemplate.queryForObject(sql, params, rowMapper)
        } catch (e: EmptyResultDataAccessException) {
            // 이미 ACTIVE라서 WHERE 조건에 안 걸려 RETURNING이 비었을 때
            findByUserAndAuthor(userId, authorId)!!
        }
    }

    // 없거나 이미 Cancelled면 no-op. 반환값은 항상 최종 상태.
    fun cancel(userId: Long, authorId: Long): Subscription? {
        val existing = findByUserAndAuthor(userId, authorId) ?: return null
        if (existing.status == SubscriptionStatus.CANCELLED) {
            return existing
        }
        val sql = """
            UPDATE subscription.subscriptions
            SET status = 'CANCELLED', cancelled_at = now()
            WHERE user_id = :userId AND author_id = :authorId AND status = 'ACTIVE'
        """.trimIndent()
        val params = MapSqlParameterSource("userId", userId).addValue("authorId", authorId)
        jdbcTemplate.update(sql, params)
        return findByUserAndAuthor(userId, authorId)
    }

    fun findActiveByUser(userId: Long, cursor: Long?, limit: Int): List<Subscription> {
        val sql = """
            SELECT id, user_id, author_id, status, subscribed_at, cancelled_at
            FROM subscription.subscriptions
            WHERE user_id = :userId AND status = 'ACTIVE' AND id > :cursor
            ORDER BY id
            LIMIT :limit
        """.trimIndent()
        val params = MapSqlParameterSource()
            .addValue("userId", userId)
            .addValue("cursor", cursor ?: 0L)
            .addValue("limit", limit)
        return jdbcTemplate.query(sql, params, rowMapper)
    }

    fun findActiveByAuthor(authorId: Long, cursor: Long?, limit: Int): List<Subscription> {
        val sql = """
            SELECT id, user_id, author_id, status, subscribed_at, cancelled_at
            FROM subscription.subscriptions
            WHERE author_id = :authorId AND status = 'ACTIVE' AND id > :cursor
            ORDER BY id
            LIMIT :limit
        """.trimIndent()
        val params = MapSqlParameterSource()
            .addValue("authorId", authorId)
            .addValue("cursor", cursor ?: 0L)
            .addValue("limit", limit)
        return jdbcTemplate.query(sql, params, rowMapper)
    }
}
