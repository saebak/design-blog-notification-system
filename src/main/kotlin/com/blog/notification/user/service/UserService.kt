package com.blog.notification.user.service

import com.blog.notification.common.ConflictException
import com.blog.notification.common.NotFoundException
import com.blog.notification.user.NotificationChannel
import com.blog.notification.user.User
import com.blog.notification.user.repository.UserRepository
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class UserService(
    private val userRepository: UserRepository,
) {
    @Transactional
    fun create(email: String, name: String, notificationChannel: NotificationChannel): User {
        if (userRepository.existsByEmail(email)) {
            throw ConflictException("Email already registered: $email")
        }
        return userRepository.save(User(email = email, name = name, notificationChannel = notificationChannel))
    }

    fun getById(id: Long): User =
        userRepository.findById(id).orElseThrow { NotFoundException("User not found: $id") }

    @Transactional
    fun updateNotificationChannel(id: Long, channel: NotificationChannel): User {
        val user = getById(id)
        return userRepository.save(user.copy(notificationChannel = channel))
    }
}
