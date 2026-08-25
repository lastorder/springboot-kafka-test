package com.example.kafkarebalance.config

import org.apache.kafka.clients.admin.NewTopic
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

@Configuration
class KafkaTopicConfig {

    @Bean
    fun slowEventsTopic(@Value("\${app.kafka.topic}") topicName: String): NewTopic =
        TopicBuilder.name(topicName)
            .partitions(3)
            .replicas(1)
            .build()
}
