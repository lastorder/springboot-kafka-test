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
 * 重要特性（也是本场景要演示的坑）：DefaultErrorHandler 的重试是在**同一个消费线程**、
 * **同一次 poll 循环内**同步进行的——重试前的等待用的是 Thread.sleep 语义的阻塞等待，
 * 并不会释放线程去调用 KafkaConsumer#poll()。因此重试耗时会直接累加到本次批次的
 * 总处理时间里，如果耗时超过 max.poll.interval.ms，同样会触发 consumer group rebalance。
 */
@Configuration
@Profile("scenario3")
class Scenario3ErrorHandlerConfig {

    private val log = LoggerFactory.getLogger(Scenario3ErrorHandlerConfig::class.java)

    @Bean
    fun errorHandler(): DefaultErrorHandler {
        // 每次重试前等待 3000ms，最多重试 3 次（不含首次尝试）
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
        private const val RETRY_INTERVAL_MS = 3000L
        private const val MAX_RETRY_ATTEMPTS = 3L
    }
}
