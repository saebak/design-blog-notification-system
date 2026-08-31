package com.blog.notification.notification.backfill

import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component
@ConditionalOnProperty(
    prefix = "notification.subscriber-read-model.backfill",
    name = ["enabled"],
    havingValue = "true",
)
class SubscriberReadModelBackfillRunner(
    private val backfillService: SubscriberReadModelBackfillService,
    @Value("\${notification.subscriber-read-model.backfill.page-size:1000}")
    private val pageSize: Int,
) : ApplicationRunner {
    override fun run(args: ApplicationArguments) {
        backfillService.backfill(pageSize)
    }
}
