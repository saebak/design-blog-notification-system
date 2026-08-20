package com.blog.notification.post.controller

import com.blog.notification.post.dto.CreatePostRequest
import com.blog.notification.post.dto.PostResponse
import com.blog.notification.post.service.PostService
import jakarta.validation.Valid
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/posts")
class PostController(
    private val postService: PostService,
) {
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    fun create(@Valid @RequestBody request: CreatePostRequest): PostResponse =
        PostResponse.from(postService.create(request.authorId, request.title, request.content))

    @GetMapping("/{id}")
    fun get(@PathVariable id: Long): PostResponse =
        PostResponse.from(postService.getById(id))

    @PostMapping("/{id}/publish")
    fun publish(@PathVariable id: Long): PostResponse =
        PostResponse.from(postService.publish(id))
}
