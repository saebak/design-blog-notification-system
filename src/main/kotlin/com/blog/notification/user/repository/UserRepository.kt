package com.blog.notification.user.repository

import com.blog.notification.user.NotificationChannel
import com.blog.notification.user.User
import org.springframework.data.repository.CrudRepository

interface UserRepository : CrudRepository<User, Long> {
    fun existsByEmail(email: String): Boolean

    // Fan-out 시 Mute 필터링용 벌크 조회.
    fun findByIdInAndNotificationChannelNot(ids: Collection<Long>, channel: NotificationChannel): List<User>
}
