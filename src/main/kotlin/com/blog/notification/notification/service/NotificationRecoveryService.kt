package com.blog.notification.notification.service

import com.blog.notification.common.ConflictException
import com.blog.notification.common.NotFoundException
import com.blog.notification.notification.operations.NotificationOperationsMetrics
import com.blog.notification.notification.repository.FanoutDispatchJdbcDao
import com.blog.notification.notification.repository.FanoutDispatchState
import com.blog.notification.notification.repository.NotificationDeliveryLogJdbcDao
import java.util.UUID
import org.springframework.stereotype.Service

@Service
class NotificationRecoveryService(
    private val fanoutDispatchDao: FanoutDispatchJdbcDao,
    private val deliveryLogDao: NotificationDeliveryLogJdbcDao,
    private val metrics: NotificationOperationsMetrics,
) {
    fun retryFanout(eventId: UUID): FanoutDispatchState {
        val current = fanoutDispatchDao.find(eventId)
            ?: throw NotFoundException("Fan-out dispatch not found: $eventId")
        if (current.status != "FAILED" || !fanoutDispatchDao.requeueFailed(eventId)) {
            throw ConflictException("Only FAILED fan-out dispatches can be retried: $eventId")
        }
        metrics.recordManualRecovery("fanout")
        return requireNotNull(fanoutDispatchDao.find(eventId))
    }

    fun retryDelivery(deliveryId: Long) {
        if (!deliveryLogDao.requeueDeadLetter(deliveryId)) {
            throw ConflictException("Only DEAD_LETTER deliveries can be retried: $deliveryId")
        }
        metrics.recordManualRecovery("push")
    }
}
