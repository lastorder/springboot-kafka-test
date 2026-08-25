package com.example.kafkarebalance.listener

import com.example.kafkarebalance.model.DemoEvent
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Profile
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * 场景四：监听 slow-events topic，模拟"下游服务临时不可用，通过指数退避重试，
 * 最终在下游恢复后成功处理，且全程不触发 consumer group rebalance"。
 *
 * 与场景三的关键区别：
 * - 场景三的"毒消息"永远失败，直到 [org.springframework.util.backoff.FixedBackOff]
 *   的重试次数耗尽被跳过；且刻意把**单次**退避等待时间设置得超过 max.poll.interval.ms，
 *   用于演示"单次等待超标即触发 rebalance"。
 * - 场景四的消息模拟"下游服务临时故障，一段时间后自行恢复"：前
 *   [RECOVERY_AFTER_ATTEMPTS] 次尝试都会失败（模拟下游不可用），
 *   达到该次数后视为"下游已恢复"，正常处理并成功提交 offset。
 *   配合 [com.example.kafkarebalance.config.Scenario4ErrorHandlerConfig] 中注册的
 *   使用 `ExponentialBackOffWithMaxRetries` 的 DefaultErrorHandler——只要**单次**
 *   退避等待的上限（maxInterval）始终小于 max.poll.interval.ms，无论总共重试多少次、
 *   总耗时多长，都不会触发 rebalance。
 */
@Component
@Profile("scenario4")
class FlakyDownstreamEventListener {

    private val log = LoggerFactory.getLogger(FlakyDownstreamEventListener::class.java)

    // 记录每条消息已经被尝试的次数，key 为消息 id；仅用于演示，非生产级实现
    // （生产环境不应依赖消费者进程内存来判断重试次数，重启后会丢失）。
    private val attemptCounters = ConcurrentHashMap<String, AtomicInteger>()

    @KafkaListener(
        topics = ["\${app.kafka.topic}"],
        groupId = "\${spring.kafka.consumer.group-id}"
    )
    fun onMessage(
        event: DemoEvent,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int,
        acknowledgment: Acknowledgment
    ) {
        if (!event.flaky) {
            log.info("收到消息 id={} partition={}（非 flaky，正常处理）", event.id, partition)
            Thread.sleep(NORMAL_PROCESS_MS)
            ackSafely(event, acknowledgment)
            return
        }

        val attempts = attemptCounters.computeIfAbsent(event.id) { AtomicInteger(0) }.incrementAndGet()
        log.info(
            "收到消息 id={} partition={} 这是第 {} 次尝试（模拟下游服务临时不可用）",
            event.id, partition, attempts
        )

        if (attempts <= RECOVERY_AFTER_ATTEMPTS) {
            log.error(
                "模拟下游仍不可用：id={} 第 {} 次尝试失败，将触发指数退避重试",
                event.id, attempts
            )
            throw TransientProcessingException("模拟下游服务临时不可用 id=${event.id} attempt=$attempts")
        }

        log.info("模拟下游已恢复：id={} 第 {} 次尝试将正常处理", event.id, attempts)
        Thread.sleep(NORMAL_PROCESS_MS)
        ackSafely(event, acknowledgment)
    }

    private fun ackSafely(event: DemoEvent, acknowledgment: Acknowledgment) {
        try {
            acknowledgment.acknowledge()
            log.info("消息处理完成并已提交 offset id={}", event.id)
        } catch (ex: Exception) {
            log.warn(
                "提交 offset 失败，可能因 rebalance 导致该分区已被回收 id={}",
                event.id, ex
            )
        }
    }

    companion object {
        private const val NORMAL_PROCESS_MS = 100L

        // 下游模拟在第 4 次尝试之前（含）都不可用，第 5 次尝试开始视为已恢复。
        // 需要小于 Scenario4ErrorHandlerConfig 中配置的 maxRetries，
        // 否则重试会在下游"恢复"之前就耗尽被跳过，无法演示"最终成功"的效果。
        private const val RECOVERY_AFTER_ATTEMPTS = 4
    }
}
