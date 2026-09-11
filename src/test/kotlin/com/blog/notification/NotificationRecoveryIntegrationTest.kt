package com.blog.notification

import com.blog.notification.notification.repository.FanoutDispatchJdbcDao
import com.blog.notification.notification.repository.NotificationDeliveryLogJdbcDao
import com.blog.notification.notification.repository.NotificationInsert
import com.blog.notification.notification.repository.NotificationJdbcDao
import com.blog.notification.notification.service.NotificationRecoveryService
import com.blog.notification.user.User
import com.blog.notification.user.repository.UserRepository
import io.micrometer.core.instrument.MeterRegistry
import java.util.UUID
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate

@Import(TestcontainersConfiguration::class)
@SpringBootTest(properties = [
    "notification.delivery.worker.enabled=false",
    "notification.fanout.retry-enabled=false",
])
class NotificationRecoveryIntegrationTest {

    @Autowired lateinit var recoveryService: NotificationRecoveryService
    @Autowired lateinit var fanoutDao: FanoutDispatchJdbcDao
    @Autowired lateinit var deliveryDao: NotificationDeliveryLogJdbcDao
    @Autowired lateinit var notificationDao: NotificationJdbcDao
    @Autowired lateinit var userRepository: UserRepository
    @Autowired lateinit var jdbcTemplate: NamedParameterJdbcTemplate
    @Autowired lateinit var meterRegistry: MeterRegistry

    @Test
    fun `failed fanout can be manually requeued and is reflected in metrics`() {
        val eventId = UUID.randomUUID()
        fanoutDao.start(eventId, 910_001L, 920_001L, "manual recovery")
        assertEquals("FAILED", fanoutDao.scheduleRetry(eventId, 1, 1, "forced failure"))
        assertTrue(gauge("notification.fanout.dispatches", "status", "failed") >= 1.0)

        val recovered = recoveryService.retryFanout(eventId)

        assertEquals("WAITING_RETRY", recovered.status)
        assertEquals(0, recovered.retryCount)
        assertEquals(1.0, meterRegistry.get("notification.manual.recovery").tag("type", "fanout").counter().count())
    }

    @Test
    fun `dead letter delivery can be manually requeued`() {
        val recipient = userRepository.save(User(email = "manual-delivery@test.com", name = "recipient"))
        val notificationId = notificationDao.insertAndGetId(
            NotificationInsert(
                recipientId = requireNotNull(recipient.id),
                sourceEventId = UUID.randomUUID(),
                postId = 910_002L,
                authorId = requireNotNull(recipient.id),
                title = "manual delivery recovery",
            ),
        )
        deliveryDao.insertPending(notificationId)
        val deliveryId = jdbcTemplate.queryForObject(
            "SELECT id FROM notification.notification_delivery_log WHERE notification_id = :notificationId",
            MapSqlParameterSource("notificationId", notificationId),
            Long::class.java,
        )!!
        jdbcTemplate.update(
            "UPDATE notification.notification_delivery_log SET status = 'DEAD_LETTER', attempt_count = 3 WHERE id = :id",
            MapSqlParameterSource("id", deliveryId),
        )
        assertTrue(gauge("notification.delivery.attempts", "status", "dead_letter") >= 1.0)

        recoveryService.retryDelivery(deliveryId)

        val state = jdbcTemplate.queryForMap(
            "SELECT status, attempt_count FROM notification.notification_delivery_log WHERE id = :id",
            MapSqlParameterSource("id", deliveryId),
        )
        assertEquals("PENDING", state["status"])
        assertEquals(0, (state["attempt_count"] as Number).toInt())
    }

    private fun gauge(name: String, tag: String, value: String): Double =
        requireNotNull(meterRegistry.get(name).tag(tag, value).gauge().value())
}
