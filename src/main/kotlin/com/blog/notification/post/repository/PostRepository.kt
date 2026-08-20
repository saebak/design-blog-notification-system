package com.blog.notification.post.repository

import com.blog.notification.post.Post
import org.springframework.data.repository.CrudRepository

interface PostRepository : CrudRepository<Post, Long>
