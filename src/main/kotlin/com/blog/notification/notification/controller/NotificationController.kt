package com.blog.notification.notification.controller

import com.blog.notification.notification.dto.MarkAllReadResponse
import com.blog.notification.notification.dto.NotificationResponse
import com.blog.notification.notification.service.NotificationService
import org.springframework.web.bind.annotation.PatchMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

// 인증/인가는 스코프 밖(docs/decisions.md §11) — recipientId를 요청 파라미터로 직접 받는다.
// 대신 recipientId 조건은 쿼리 자체에 강제해(NotificationJdbcDao) 타 사용자 알림에 접근 못하게 한다.
@RestController
class NotificationController(
    private val notificationService: NotificationService,
) {
    @PatchMapping("/api/notifications/{id}/read")
    fun markAsRead(
        @PathVariable id: Long,
        @RequestParam recipientId: Long,
    ): NotificationResponse =
        NotificationResponse.from(notificationService.markAsRead(recipientId, id))

    @PatchMapping("/api/users/{recipientId}/notifications/read-all")
    fun markAllAsRead(@PathVariable recipientId: Long): MarkAllReadResponse =
        MarkAllReadResponse(notificationService.markAllAsRead(recipientId))
}
