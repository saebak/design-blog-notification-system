package com.blog.notification.user.dto

import com.blog.notification.user.NotificationChannel
import com.blog.notification.user.User
import jakarta.validation.constraints.Email
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.NotNull
import java.time.Instant

data class CreateUserRequest(
    @field:NotBlank @field:Email val email: String,
    @field:NotBlank val name: String,
    val notificationChannel: NotificationChannel = NotificationChannel.PUSH,
)

data class UpdateNotificationChannelRequest(
    @field:NotNull val notificationChannel: NotificationChannel,
)

data class UserResponse(
    val id: Long,
    val email: String,
    val name: String,
    val notificationChannel: NotificationChannel,
    val createdAt: Instant,
) {
    companion object {
        fun from(user: User) = UserResponse(
            id = requireNotNull(user.id),
            email = user.email,
            name = user.name,
            notificationChannel = user.notificationChannel,
            createdAt = user.createdAt,
        )
    }
}
