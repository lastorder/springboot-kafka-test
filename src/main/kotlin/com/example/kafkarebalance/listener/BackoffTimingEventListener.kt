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
 * 场景五：专用于验证"处理耗时是否会被退避等待时间扣除"这一问题。
 *
 * 对标记 `flaky=true` 的消息：
 * - 前 [FAIL_ATTEMPTS] 次尝试，先人为耗时 4000ms（模拟"业务逻辑已经执行了一部分，
 *   比如已经调用下游、等待超时前"），再抛出可重试异常；
 * - 之后视为"最终成功"，正常处理并提交 offset。
 *
 * 配合 [com.example.kafkarebalance.config.Scenario5ErrorHandlerConfig] 中注册的
 * 固定退避（10000ms）ErrorHandler，实测比对：
 * - 若"下一次重新处理"发生在"本次处理开始时刻 + 处理耗时(4s) + 退避时间(10s) = 14s"附近，
 *   证明退避是从"处理结束/抛出异常的那一刻"开始独立计时整整 10s（不扣除已耗费的 4s）；
 * - 若发生在"本次处理开始时刻 + 10s"附近（即退避时间里已经包含了处理耗时），
 *   证明退避时间会扣除处理耗时。
 *
 * 每条日志都打印精确的开始/结束时间戳（通过 System.currentTimeMillis()），
 * 便于在归档日志中直接计算实际间隔，不依赖日志本身的打印时刻误差。
 */
@Component
@Profile("scenario5")
class BackoffTimingEventListener {

    private val log = LoggerFactory.getLogger(BackoffTimingEventListener::class.java)

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
            ackSafely(event, acknowledgment)
            return
        }

        val attempts = attemptCounters.computeIfAbsent(event.id) { AtomicInteger(0) }.incrementAndGet()
        val startMs = System.currentTimeMillis()
        log.info(
            "收到消息 id={} partition={} 第 {} 次尝试，开始处理，startEpochMs={}",
            event.id, partition, attempts, startMs
        )

        if (attempts <= FAIL_ATTEMPTS) {
            // 人为耗费 4000ms 模拟"业务逻辑已执行一部分才失败"（例如调用下游、等待超时）
            Thread.sleep(Scenario5ProcessingTime.PROCESSING_MS)
            val throwMs = System.currentTimeMillis()
            log.error(
                "模拟处理失败：id={} 第 {} 次尝试在耗费 {}ms 处理后抛出异常，throwEpochMs={}（实际耗时={}ms）",
                event.id, attempts, Scenario5ProcessingTime.PROCESSING_MS, throwMs, throwMs - startMs
            )
            throw TransientProcessingException("模拟处理耗时后失败 id=${event.id} attempt=$attempts")
        }

        val endMs = System.currentTimeMillis()
        log.info(
            "第 {} 次尝试成功：id={} endEpochMs={}（距离本次处理开始耗时={}ms）",
            attempts, event.id, endMs, endMs - startMs
        )
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
        // 前 2 次尝试模拟处理耗时后失败，第 3 次尝试视为成功
        private const val FAIL_ATTEMPTS = 2
    }
}

/**
 * 避免在两个文件间产生循环引用，单独暴露处理耗时常量供本监听器使用；
 * 与 [com.example.kafkarebalance.config.Scenario5ErrorHandlerConfig.PROCESSING_MS] 保持一致。
 */
object Scenario5ProcessingTime {
    const val PROCESSING_MS = 4_000L
}
