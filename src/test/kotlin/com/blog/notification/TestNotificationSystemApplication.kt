package com.blog.notification

import org.springframework.boot.fromApplication
import org.springframework.boot.with


fun main(args: Array<String>) {
	fromApplication<NotificationSystemApplication>().with(TestcontainersConfiguration::class).run(*args)
}
