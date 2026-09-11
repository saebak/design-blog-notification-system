package com.blog.notification.notification.operations

import com.blog.notification.notification.repository.FanoutDispatchJdbcDao
import com.blog.notification.notification.repository.NotificationDeliveryLogJdbcDao
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Component

@Component
class NotificationOperationsMetrics(
    private val registry: MeterRegistry,
    fanoutDispatchDao: FanoutDispatchJdbcDao,
    deliveryLogDao: NotificationDeliveryLogJdbcDao,
) {
    init {
        listOf("IN_PROGRESS", "WAITING_RETRY", "FAILED").forEach { status ->
            Gauge.builder("notification.fanout.dispatches", fanoutDispatchDao) {
                it.countByStatus(status).toDouble()
            }.tag("status", status.lowercase()).register(registry)
        }
        listOf("PENDING", "PROCESSING", "FAILED", "DEAD_LETTER").forEach { status ->
            Gauge.builder("notification.delivery.attempts", deliveryLogDao) {
                it.countByStatus(status).toDouble()
            }.tag("channel", "push").tag("status", status.lowercase()).register(registry)
        }
    }

    fun recordManualRecovery(type: String) {
        registry.counter("notification.manual.recovery", "type", type).increment()
    }

    fun recordFanoutChunkDispatched() = registry.counter("notification.fanout.chunks.dispatched").increment()

    fun recordFanoutChunkCompleted() = registry.counter("notification.fanout.chunks.completed").increment()

    fun recordFanoutDeferred() = registry.counter("notification.fanout.dispatch.deferred").increment()

    fun recordFanoutFailed() = registry.counter("notification.fanout.dispatch.failed").increment()

    fun recordDeliveryClaimed(count: Int) = registry.counter("notification.delivery.claimed", "channel", "push")
        .increment(count.toDouble())

    fun recordDeliverySent() = registry.counter("notification.delivery.sent", "channel", "push").increment()

    fun recordDeliveryFailed(deadLetter: Boolean) = registry.counter(
        "notification.delivery.failed",
        "channel", "push",
        "terminal", deadLetter.toString(),
    ).increment()
}
