package com.example.kafkarebalance.config

import org.apache.kafka.clients.admin.NewTopic
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.kafka.config.TopicBuilder

@Configuration
class KafkaTopicConfig {

    @Bean
    fun slowEventsTopic(@Value("\${app.kafka.topic}") topicName: String): NewTopic =
        TopicBuilder.name(topicName)
            .partitions(3)
            .replicas(1)
            .build()

    /**
     * 场景六、七专用：第二个 topic，与 `slow-events` 由**同一个 consumer group** 消费。
     * 场景六用于验证"一个多 topic `@KafkaListener` 中，一个 topic 触发 rebalance
     * 是否会牵连另一个 topic"；场景七用于验证"拆分成两个各自独立的
     * `@KafkaListener`（仍共享 groupId）后，是否能避免这种牵连"。
     */
    @Bean
    @Profile("scenario6", "scenario7")
    fun otherEventsTopic(@Value("\${app.kafka.other-topic:other-events}") topicName: String): NewTopic =
        TopicBuilder.name(topicName)
            .partitions(3)
            .replicas(1)
            .build()
}
