package com.example.kafkarebalance.config

import com.example.kafkarebalance.listener.Scenario5ProcessingTime
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.listener.RetryListener
import org.springframework.util.backoff.FixedBackOff

/**
 * 场景五专用：用于精确回答一个关于 DefaultErrorHandler 退避计时的问题——
 *
 * **"如果处理消息本身已经花费了 N 秒，然后抛出异常触发退避等待 M 秒，
 * 那么距离下一次重新调用 poll() 的实际间隔，是 M 秒，还是 (M-N) 秒？"**
 *
 * 通过阅读 spring-kafka 4.0.4 源码（[org.springframework.kafka.listener.DefaultBackOffHandler]、
 * [org.springframework.kafka.listener.FailedRecordTracker]）可以看到：
 * `nextBackOff` 的值直接来自 `BackOffExecution#nextBackOff()`（即配置的固定/指数间隔本身），
 * 并原样传给 `Thread.sleep(nextBackOff)`（或 `ListenerUtils.stoppableSleep`）——
 * **代码中没有任何地方减去"本次处理已消耗的时间"**。
 *
 * 本场景通过让消息处理先人为耗时 [PROCESSING_MS]（4000ms）再抛出异常，
 * 配合固定退避 [BACKOFF_MS]（10000ms），实测验证：
 * - 若下一次重试发生在"抛出异常时刻 + 10000ms"左右 → 证明退避时间是**独立**计时的
 *   （不减去已耗费的处理时间）；
 * - 若发生在"抛出异常时刻 + 6000ms"左右 → 证明退避时间会扣除已耗费的处理时间。
 */
@Configuration
@Profile("scenario5")
class Scenario5ErrorHandlerConfig {

    private val log = LoggerFactory.getLogger(Scenario5ErrorHandlerConfig::class.java)

    @Bean
    fun errorHandler(): DefaultErrorHandler {
        val backOff = FixedBackOff(BACKOFF_MS, MAX_ATTEMPTS)
        val errorHandler = DefaultErrorHandler(backOff)
        errorHandler.setRetryListeners(object : RetryListener {
            override fun failedDelivery(
                record: org.apache.kafka.clients.consumer.ConsumerRecord<*, *>,
                ex: Exception?,
                deliveryAttempt: Int
            ) {
                log.warn(
                    "消息 offset={} partition={} 第 {} 次投递失败（本次处理耗时约 {}ms 后抛出异常），" +
                        "接下来将退避等待 {}ms：{}",
                    record.offset(), record.partition(), deliveryAttempt,
                    Scenario5ProcessingTime.PROCESSING_MS, BACKOFF_MS, ex?.message
                )
            }
        })
        log.info(
            "已注册固定退避 ErrorHandler（用于验证退避计时是否独立于处理耗时）：" +
                "processingBeforeThrow={}ms backoff={}ms maxAttempts={}",
            Scenario5ProcessingTime.PROCESSING_MS, BACKOFF_MS, MAX_ATTEMPTS
        )
        return errorHandler
    }

    companion object {
        // 固定退避等待时间；刻意选择与 Scenario5ProcessingTime.PROCESSING_MS（4s）有显著差异（10s），
        // 便于从日志时间戳上一眼区分"退避是否独立计时"
        private const val BACKOFF_MS = 10_000L
        private const val MAX_ATTEMPTS = 3L
    }
}
