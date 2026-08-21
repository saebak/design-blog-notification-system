package com.blog.notification.notification.gateway

// 외부 Push 서버로 발송을 위임하는 지점 — domain-design.md §5.4.
// 이 시스템의 책임은 이 인터페이스를 호출하는 데까지고, 실제 FCM/APNs 연동은 그 서버의 책임이다.
fun interface PushGatewayPort {
    fun send(recipientId: Long, title: String): Boolean
}
