package com.blog.notification

import com.blog.notification.notification.repository.NotificationDeliveryLogJdbcDao
import com.blog.notification.notification.repository.NotificationInsert
import com.blog.notification.notification.repository.NotificationJdbcDao
import com.blog.notification.user.User
import com.blog.notification.user.repository.UserRepository
import java.util.UUID
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate

@Import(TestcontainersConfiguration::class)
@SpringBootTest(properties = ["notification.delivery.worker.enabled=false"])
class DeliveryClaimIntegrationTest {

    @Autowired lateinit var userRepository: UserRepository
    @Autowired lateinit var notificationDao: NotificationJdbcDao
    @Autowired lateinit var deliveryLogDao: NotificationDeliveryLogJdbcDao
    @Autowired lateinit var jdbcTemplate: NamedParameterJdbcTemplate

    @Test
    fun `concurrent workers claim a pending delivery only once`() {
        val recipient = userRepository.save(User(email = "delivery-claim@test.com", name = "recipient"))
        val notificationId = notificationDao.insertAndGetId(
            NotificationInsert(
                recipientId = requireNotNull(recipient.id),
                sourceEventId = UUID.randomUUID(),
                postId = 800_001L,
                authorId = requireNotNull(recipient.id),
                title = "claim",
            ),
        )
        deliveryLogDao.insertPending(notificationId)
        val ready = CountDownLatch(2)
        val start = CountDownLatch(1)
        val executor = Executors.newFixedThreadPool(2)

        val claimed = try {
            val futures = (1..2).map {
                executor.submit(Callable {
                    ready.countDown()
                    start.await()
                    deliveryLogDao.claimDue(limit = 1, maxAttempts = 3, leaseSeconds = 30)
                })
            }
            ready.await()
            start.countDown()
            futures.flatMap { it.get() }
        } finally {
            executor.shutdownNow()
        }

        assertEquals(1, claimed.size)
        assertEquals(notificationId, claimed.single().notificationId)
    }

    @Test
    fun `expired claim is recovered and stale owner cannot complete it`() {
        val recipient = userRepository.save(User(email = "delivery-expired@test.com", name = "recipient"))
        val notificationId = notificationDao.insertAndGetId(
            NotificationInsert(
                recipientId = requireNotNull(recipient.id),
                sourceEventId = UUID.randomUUID(),
                postId = 800_002L,
                authorId = requireNotNull(recipient.id),
                title = "expired claim",
            ),
        )
        deliveryLogDao.insertPending(notificationId)
        val staleClaim = deliveryLogDao.claimDue(1, 3, 30).single()
        jdbcTemplate.update(
            "UPDATE notification.notification_delivery_log SET claimed_until = now() - interval '1 second' WHERE id = :id",
            MapSqlParameterSource("id", staleClaim.id),
        )

        val recoveredClaim = deliveryLogDao.claimDue(1, 3, 30).single()

        assertEquals(staleClaim.id, recoveredClaim.id)
        assertEquals(false, deliveryLogDao.markSent(staleClaim.id, staleClaim.claimToken))
        assertEquals(true, deliveryLogDao.markSent(recoveredClaim.id, recoveredClaim.claimToken))
        assertEquals("SENT", deliveryStatus(recoveredClaim.id))
    }

    private fun deliveryStatus(id: Long): String = jdbcTemplate.queryForObject(
        "SELECT status FROM notification.notification_delivery_log WHERE id = :id",
        MapSqlParameterSource("id", id),
        String::class.java,
    )!!
}
