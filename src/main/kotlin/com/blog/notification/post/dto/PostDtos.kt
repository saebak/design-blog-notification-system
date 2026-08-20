package com.blog.notification.post.dto

import com.blog.notification.post.Post
import com.blog.notification.post.PostStatus
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.NotNull
import java.time.Instant

data class CreatePostRequest(
    @field:NotNull val authorId: Long,
    @field:NotBlank val title: String,
    @field:NotBlank val content: String,
)

data class PostResponse(
    val id: Long,
    val authorId: Long,
    val title: String,
    val content: String,
    val status: PostStatus,
    val publishedAt: Instant?,
    val createdAt: Instant,
    val updatedAt: Instant,
) {
    companion object {
        fun from(post: Post) = PostResponse(
            id = requireNotNull(post.id),
            authorId = post.authorId,
            title = post.title,
            content = post.content,
            status = post.status,
            publishedAt = post.publishedAt,
            createdAt = post.createdAt,
            updatedAt = post.updatedAt,
        )
    }
}
