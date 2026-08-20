package com.blog.notification.user.controller

import com.blog.notification.user.dto.CreateUserRequest
import com.blog.notification.user.dto.UpdateNotificationChannelRequest
import com.blog.notification.user.dto.UserResponse
import com.blog.notification.user.service.UserService
import jakarta.validation.Valid
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PatchMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/users")
class UserController(
    private val userService: UserService,
) {
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    fun create(@Valid @RequestBody request: CreateUserRequest): UserResponse {
        val user = userService.create(request.email, request.name, request.notificationChannel)
        return UserResponse.from(user)
    }

    @GetMapping("/{id}")
    fun get(@PathVariable id: Long): UserResponse =
        UserResponse.from(userService.getById(id))

    @PatchMapping("/{id}/notification-channel")
    fun updateNotificationChannel(
        @PathVariable id: Long,
        @Valid @RequestBody request: UpdateNotificationChannelRequest,
    ): UserResponse {
        val user = userService.updateNotificationChannel(id, request.notificationChannel)
        return UserResponse.from(user)
    }
}
