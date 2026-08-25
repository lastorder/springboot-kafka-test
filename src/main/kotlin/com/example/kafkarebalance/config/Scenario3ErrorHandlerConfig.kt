package com.example.kafkarebalance.config

import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.util.backoff.FixedBackOff

/**
 * 场景三专用：注册一个使用阻塞式 [FixedBackOff] 的 [DefaultErrorHandler]。
 *
 * 重要说明（已通过实测校正）：Spring Kafka 的 DefaultErrorHandler 在两次重试之间
 * 会重新调用 KafkaConsumer#poll()（哪怕本地队列已有数据），这意味着只要
 * **单次重试等待时间**本身小于 max.poll.interval.ms，就不会触发 rebalance——
 * 因为每次 poll() 都会重置"距离上次 poll 的时间"这个计时器。
 *
 * 因此要复现"重试导致 rebalance"，必须让**单次**重试等待时间本身就超过
 * max.poll.interval.ms（而不是指望多次重试的总耗时累加触发）。
 * 本场景将 backoff 间隔设置为 9000ms，超过 application-scenario3.yml 中的
 * max.poll.interval.ms=8000ms，从而在第一次重试等待期间就会触发 rebalance。
 */
@Configuration
@Profile("scenario3")
class Scenario3ErrorHandlerConfig {

    private val log = LoggerFactory.getLogger(Scenario3ErrorHandlerConfig::class.java)

    @Bean
    fun errorHandler(): DefaultErrorHandler {
        // 单次重试等待 9000ms，超过 max.poll.interval.ms=8000ms；最多重试 3 次
        val backOff = FixedBackOff(RETRY_INTERVAL_MS, MAX_RETRY_ATTEMPTS)
        val errorHandler = DefaultErrorHandler(backOff)
        errorHandler.setRetryListeners(object : org.springframework.kafka.listener.RetryListener {
            override fun failedDelivery(record: org.apache.kafka.clients.consumer.ConsumerRecord<*, *>, ex: Exception?, deliveryAttempt: Int) {
                log.warn(
                    "消息 offset={} partition={} 第 {} 次投递失败，将进行阻塞式重试等待 {}ms：{}",
                    record.offset(), record.partition(), deliveryAttempt, RETRY_INTERVAL_MS, ex?.message
                )
            }
        })
        return errorHandler
    }

    companion object {
        private const val RETRY_INTERVAL_MS = 9000L
        private const val MAX_RETRY_ATTEMPTS = 3L
    }
}
