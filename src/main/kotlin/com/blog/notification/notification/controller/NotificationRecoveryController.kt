package com.blog.notification.notification.controller

import com.blog.notification.notification.repository.FanoutDispatchState
import com.blog.notification.notification.service.NotificationRecoveryService
import java.util.UUID
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/internal/notification-recovery")
class NotificationRecoveryController(
    private val recoveryService: NotificationRecoveryService,
) {
    @PostMapping("/fanout/{eventId}")
    @ResponseStatus(HttpStatus.ACCEPTED)
    fun retryFanout(@PathVariable eventId: UUID): FanoutDispatchState =
        recoveryService.retryFanout(eventId)

    @PostMapping("/delivery/{deliveryId}")
    @ResponseStatus(HttpStatus.ACCEPTED)
    fun retryDelivery(@PathVariable deliveryId: Long) {
        recoveryService.retryDelivery(deliveryId)
    }
}
