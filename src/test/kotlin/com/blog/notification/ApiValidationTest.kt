package com.blog.notification

import com.blog.notification.common.ApiExceptionHandler
import com.blog.notification.post.controller.PostController
import com.blog.notification.post.service.PostService
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.post
import org.springframework.test.web.servlet.setup.MockMvcBuilders

class ApiValidationTest {

    private val mockMvc = MockMvcBuilders
        .standaloneSetup(PostController(mock(PostService::class.java)))
        .setControllerAdvice(ApiExceptionHandler())
        .build()

    @Test
    fun `post creation rejects non-positive author and oversized title`() {
        val oversizedTitle = "a".repeat(201)

        mockMvc.post("/api/posts") {
            contentType = MediaType.APPLICATION_JSON
            content = """{"authorId":0,"title":"$oversizedTitle","content":"content"}"""
        }.andExpect {
            status { isBadRequest() }
            jsonPath("$.status") { value(400) }
        }
    }
}
