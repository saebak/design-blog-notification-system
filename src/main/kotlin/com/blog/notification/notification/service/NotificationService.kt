package com.blog.notification.notification.service

import com.blog.notification.common.NotFoundException
import com.blog.notification.notification.Notification
import com.blog.notification.notification.repository.NotificationJdbcDao
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class NotificationService(
    private val notificationDao: NotificationJdbcDao,
) {
    @Transactional
    fun markAsRead(recipientId: Long, id: Long): Notification =
        notificationDao.markRead(id, recipientId)
            ?: throw NotFoundException("Notification not found: id=$id, recipientId=$recipientId")

    @Transactional
    fun markAllAsRead(recipientId: Long): Int = notificationDao.markAllRead(recipientId)
}
