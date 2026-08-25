package com.example.kafkarebalance.config

import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.listener.RetryListener
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries

/**
 * 场景四专用：注册一个使用 [ExponentialBackOffWithMaxRetries] 的 [DefaultErrorHandler]，
 * 模拟"下游服务临时不可用，通过指数退避重试，最终成功且不触发 rebalance"。
 *
 * 核心原理（已在场景三通过实测验证）：`DefaultErrorHandler` 在每次重试之间都会
 * 重新调用 `KafkaConsumer#poll()`，这会重置"距离上次成功 poll 的时间"这个计时器。
 * 因此触发 rebalance 的条件是**单次**退避等待时间超过 `max.poll.interval.ms`，
 * 而与"总共重试了多少次、累计等待了多长时间"无关。
 *
 * 本场景反过来利用这一机制：只要把退避的**单次上限**（maxInterval）控制在
 * `max.poll.interval.ms` 以内（并留出安全余量），无论重试多少次、
 * 总耗时多长（哪怕是生产环境的 15 分钟），都不会触发 rebalance。
 *
 * 本地实测使用"时间压缩版"参数（秒级），使得单次运行可以在几十秒内完整验证整个链路；
 * 生产环境的真实参数（分钟级，总预算约 15 分钟）见 docs/rebalance-analysis.md
 * 场景四章节的"生产环境最优配置"说明，两者的比例关系一致，只是量纲不同。
 *
 * 压缩版参数推导：initialInterval=500ms, multiplier=2.0, maxInterval=3000ms，
 * maxRetries=8 时，各次等待依次为 500/1000/2000/3000/3000/3000/3000/3000ms，
 * 累计约 18.5s（对应生产环境的"最多等待 15 分钟"）。maxInterval=3000ms 明显小于
 * application-scenario4.yml 中 max.poll.interval.ms=6000ms（2 倍安全余量）。
 */
@Configuration
@Profile("scenario4")
class Scenario4ErrorHandlerConfig {

    private val log = LoggerFactory.getLogger(Scenario4ErrorHandlerConfig::class.java)

    @Bean
    fun errorHandler(): DefaultErrorHandler {
        val backOff = ExponentialBackOffWithMaxRetries(MAX_RETRIES).apply {
            initialInterval = INITIAL_INTERVAL_MS
            multiplier = MULTIPLIER
            maxInterval = MAX_INTERVAL_MS
        }
        val errorHandler = DefaultErrorHandler(backOff)
        errorHandler.setRetryListeners(object : RetryListener {
            override fun failedDelivery(
                record: org.apache.kafka.clients.consumer.ConsumerRecord<*, *>,
                ex: Exception?,
                deliveryAttempt: Int
            ) {
                log.warn(
                    "消息 offset={} partition={} 第 {} 次投递失败，将进行指数退避重试：{}",
                    record.offset(), record.partition(), deliveryAttempt, ex?.message
                )
            }
        })
        log.info(
            "已注册指数退避 ErrorHandler：initialInterval={}ms multiplier={} maxInterval={}ms maxRetries={}",
            INITIAL_INTERVAL_MS, MULTIPLIER, MAX_INTERVAL_MS, MAX_RETRIES
        )
        return errorHandler
    }

    companion object {
        // ---- 本地实测用的"时间压缩版"参数（秒级，约 18.5s 总预算） ----
        private const val INITIAL_INTERVAL_MS = 500L
        private const val MULTIPLIER = 2.0
        private const val MAX_INTERVAL_MS = 3_000L
        private const val MAX_RETRIES = 8
    }
}
