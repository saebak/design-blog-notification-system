package com.blog.notification.subscription

/**
 * Subscription Bounded Context — 구독/구독취소, SubscriptionChanged 이벤트 발행 (docs/domain-design.md §4).
 * 다른 Context의 패키지를 참조하지 않는다 — Context 간 통신은 이벤트로만 한다.
 */
internal object SubscriptionContext
