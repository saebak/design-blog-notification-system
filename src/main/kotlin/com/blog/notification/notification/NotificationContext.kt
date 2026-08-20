package com.blog.notification.notification

/**
 * Notification Bounded Context — Fan-out(Dispatcher/Chunk Worker), 발송, 인앱 알림/읽음 처리,
 * 실시간 채널 (docs/domain-design.md §5, docs/architecture.md §4~§6).
 * Post/Subscription의 이벤트만 구독하고, 다른 Context의 패키지를 직접 참조하지 않는다.
 */
internal object NotificationContext
