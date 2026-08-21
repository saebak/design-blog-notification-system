package com.blog.notification.notification.gateway

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

// 목업 구현 — 실제 Push 서버 연동은 FR-4.1 범위 밖.
// 나중에 실제 HTTP 연동으로 교체할 때 이 클래스만 PushGatewayPort 구현체로 갈아끼우면 된다.
@Component
class SimulatedPushGatewayAdapter : PushGatewayPort {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun send(recipientId: Long, title: String): Boolean {
        log.info("외부 Push 서버로 발송 위임 (simulated): recipientId={}, title={}", recipientId, title)
        return true
    }
}
