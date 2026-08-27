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
     * 场景六专用：第二个 topic，与 `slow-events` 由**同一个 consumer group** 消费。
     * 用于验证"其中一个 topic 的消息处理异常触发 rebalance 时，是否会影响
     * 同一 consumer group 内、正在正常消费的另一个 topic"。
     */
    @Bean
    @Profile("scenario6")
    fun otherEventsTopic(@Value("\${app.kafka.other-topic:other-events}") topicName: String): NewTopic =
        TopicBuilder.name(topicName)
            .partitions(3)
            .replicas(1)
            .build()
}
