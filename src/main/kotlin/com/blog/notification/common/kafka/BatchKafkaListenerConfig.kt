package com.blog.notification.common.kafka

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory

// SubscriberSyncConsumer처럼 폴링 배치 단위로 묶어서 처리해야 하는 리스너 전용 컨테이너 팩토리.
// 기본 팩토리(단건 리스너)와 분리해, PostPublishedFanoutConsumer 같은 단건 처리 리스너에는
// 영향을 주지 않는다.
// 자동설정된 ConsumerFactory 빈을 그대로 재사용한다 — 직접 KafkaProperties로 ConsumerFactory를
// 새로 만들면 Testcontainers의 KafkaConnectionDetails 메커니즘(동적 포트 주입)을 우회하게 되어,
// 테스트에서 실제 애플리케이션이 쓰는 브로커가 아니라 application.yml의 고정 주소로 연결을
// 시도하다가 컨슈머가 파티션을 영영 할당받지 못하는 문제를 실제로 겪었다.
@Configuration
class BatchKafkaListenerConfig(
    private val consumerFactory: ConsumerFactory<String, String>,
) {

    @Bean
    fun batchListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.setConsumerFactory(consumerFactory)
        factory.setBatchListener(true)
        return factory
    }
}
