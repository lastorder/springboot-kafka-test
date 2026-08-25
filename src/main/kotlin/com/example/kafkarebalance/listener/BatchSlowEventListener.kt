package com.example.kafkarebalance.listener

import com.example.kafkarebalance.model.DemoEvent
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Profile
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicInteger

/**
 * 场景二：监听 slow-events topic，模拟"单条不慢，但一次拉取太多条，累计耗时超时"。
 *
 * 每条消息本身只处理 800ms（并不慢），但 application-scenario2.yml 中
 * max.poll.records=10，Spring Kafka 容器会在一次 poll() 中拉取最多 10 条消息，
 * 并在同一个消费线程内逐条同步派发给本方法处理（这是默认行为，无需开启批量监听）。
 *
 * 计算：10 条 * 800ms = 8000ms，超过 max.poll.interval.ms=6000ms —— 触发 rebalance。
 * 而如果 max.poll.records=1，单条 800ms 远小于 6000ms，不会 rebalance——
 * 这正是"看似每条都不慢，但批量数量本身就是隐患"的经典场景。
 */
@Component
@Profile("scenario2")
class BatchSlowEventListener {

    private val log = LoggerFactory.getLogger(BatchSlowEventListener::class.java)

    // 用于统计当前 poll 批次内已处理的消息数量与累计耗时，仅用于日志展示，非线程安全敏感场景
    private val processedInBatch = AtomicInteger(0)
    private var batchStartedAtMs: Long = 0L

    @KafkaListener(
        topics = ["\${app.kafka.topic}"],
        groupId = "\${spring.kafka.consumer.group-id}"
    )
    fun onMessage(
        event: DemoEvent,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int,
        acknowledgment: Acknowledgment
    ) {
        val countInBatch = processedInBatch.incrementAndGet()
        if (countInBatch == 1) {
            batchStartedAtMs = System.currentTimeMillis()
        }

        log.info(
            "收到消息 id={} partition={} 本批次第 {} 条，处理耗时固定 {}ms",
            event.id, partition, countInBatch, PER_MESSAGE_PROCESS_MS
        )
        Thread.sleep(PER_MESSAGE_PROCESS_MS)

        val elapsedInBatch = System.currentTimeMillis() - batchStartedAtMs
        log.warn(
            "消息 id={} 处理完成，本批次累计耗时约 {}ms（第 {} 条）",
            event.id, elapsedInBatch, countInBatch
        )

        try {
            acknowledgment.acknowledge()
            log.info("消息处理完成并已提交 offset id={}", event.id)
        } catch (ex: Exception) {
            log.warn(
                "提交 offset 失败，可能因 rebalance 导致该分区已被回收 id={}",
                event.id, ex
            )
        } finally {
            // 简单地以“连续处理数量重置”近似一个新 poll 批次的开始；仅用于演示日志，不影响 Kafka 行为本身
            if (countInBatch >= RESET_THRESHOLD) {
                processedInBatch.set(0)
            }
        }
    }

    companion object {
        private const val PER_MESSAGE_PROCESS_MS = 800L
        private const val RESET_THRESHOLD = 10
    }
}
