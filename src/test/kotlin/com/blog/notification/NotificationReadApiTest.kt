package com.blog.notification

import com.blog.notification.notification.repository.NotificationInsert
import com.blog.notification.notification.repository.NotificationJdbcDao
import com.blog.notification.user.User
import com.blog.notification.user.repository.UserRepository
import java.util.UUID
import org.hamcrest.Matchers.`is`
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status

@Import(TestcontainersConfiguration::class)
@SpringBootTest
@AutoConfigureMockMvc
class NotificationReadApiTest {

    @Autowired
    lateinit var mockMvc: MockMvc

    @Autowired
    lateinit var userRepository: UserRepository

    @Autowired
    lateinit var notificationDao: NotificationJdbcDao

    @Test
    fun `개별 알림을 읽음 처리한다`() {
        val author = userRepository.save(User(email = "read-author@test.com", name = "author"))
        val recipient = userRepository.save(User(email = "read-recipient@test.com", name = "recipient"))
        val notificationId = notificationDao.insertAndGetId(
            NotificationInsert(
                recipientId = requireNotNull(recipient.id),
                sourceEventId = UUID.randomUUID(),
                postId = 1L,
                authorId = requireNotNull(author.id),
                title = "title",
            ),
        )

        mockMvc.perform(patch("/api/notifications/$notificationId/read?recipientId=${recipient.id}"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.id", `is`(notificationId.toInt())))
            .andExpect(jsonPath("$.isRead", `is`(true)))
            .andExpect(jsonPath("$.readAt").exists())
    }

    @Test
    fun `타 사용자의 알림은 읽음 처리할 수 없다`() {
        val author = userRepository.save(User(email = "read-author2@test.com", name = "author2"))
        val recipient = userRepository.save(User(email = "read-recipient2@test.com", name = "recipient2"))
        val stranger = userRepository.save(User(email = "read-stranger@test.com", name = "stranger"))
        val notificationId = notificationDao.insertAndGetId(
            NotificationInsert(
                recipientId = requireNotNull(recipient.id),
                sourceEventId = UUID.randomUUID(),
                postId = 1L,
                authorId = requireNotNull(author.id),
                title = "title",
            ),
        )

        mockMvc.perform(patch("/api/notifications/$notificationId/read?recipientId=${stranger.id}"))
            .andExpect(status().isNotFound)
    }

    @Test
    fun `전체 알림을 읽음 처리하면 이미 읽은 알림은 다시 갱신되지 않는다`() {
        val author = userRepository.save(User(email = "read-author3@test.com", name = "author3"))
        val recipient = userRepository.save(User(email = "read-recipient3@test.com", name = "recipient3"))
        val notifications = (1..3).map {
            notificationDao.insertAndGetId(
                NotificationInsert(
                    recipientId = requireNotNull(recipient.id),
                    sourceEventId = UUID.randomUUID(),
                    postId = it.toLong(),
                    authorId = requireNotNull(author.id),
                    title = "title-$it",
                ),
            )
        }
        // 미리 하나는 읽음 처리 — markAllRead의 대상에서 자연히 빠져야 한다.
        notificationDao.markRead(notifications[0], requireNotNull(recipient.id))

        mockMvc.perform(patch("/api/users/${recipient.id}/notifications/read-all"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.updatedCount", `is`(2)))

        val secondCall = notificationDao.markAllRead(requireNotNull(recipient.id))
        assertEquals(0, secondCall)
    }
}
