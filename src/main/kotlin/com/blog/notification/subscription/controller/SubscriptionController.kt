package com.blog.notification.subscription.controller

import com.blog.notification.subscription.dto.SubscribeRequest
import com.blog.notification.subscription.dto.SubscriptionResponse
import com.blog.notification.subscription.service.SubscriptionService
import jakarta.validation.Valid
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController

@RestController
class SubscriptionController(
    private val subscriptionService: SubscriptionService,
) {
    @PostMapping("/api/subscriptions")
    @ResponseStatus(HttpStatus.CREATED)
    fun subscribe(@Valid @RequestBody request: SubscribeRequest): SubscriptionResponse =
        SubscriptionResponse.from(subscriptionService.subscribe(request.userId, request.authorId))

    @DeleteMapping("/api/subscriptions")
    fun cancel(
        @RequestParam userId: Long,
        @RequestParam authorId: Long,
    ): SubscriptionResponse =
        SubscriptionResponse.from(subscriptionService.cancel(userId, authorId))

    @GetMapping("/api/users/{userId}/subscriptions")
    fun listMySubscriptions(
        @PathVariable userId: Long,
        @RequestParam(required = false) cursor: Long?,
        @RequestParam(defaultValue = "50") limit: Int,
    ): List<SubscriptionResponse> =
        subscriptionService.listMySubscriptions(userId, cursor, limit).map(SubscriptionResponse::from)

    // 내부용 벌크 조회 — 팬아웃 백필 등에서 씀
    @GetMapping("/api/authors/{authorId}/subscribers")
    fun listSubscribers(
        @PathVariable authorId: Long,
        @RequestParam(required = false) cursor: Long?,
        @RequestParam(defaultValue = "1000") limit: Int,
    ): List<SubscriptionResponse> =
        subscriptionService.listSubscribers(authorId, cursor, limit).map(SubscriptionResponse::from)
}
