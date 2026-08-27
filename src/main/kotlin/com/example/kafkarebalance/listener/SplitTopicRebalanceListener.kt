package com.example.kafkarebalance.listener

import com.example.kafkarebalance.model.DemoEvent
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Profile
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.stereotype.Component

/**
 * 场景七：验证"把场景六的单个多 topic `@KafkaListener` 拆分为两个各自独立的
 * `@KafkaListener`（每个只监听一个 topic，但共享同一个 groupId）"，
 * 是否能让一个 topic 的 rebalance 不再影响另一个 topic。
 *
 * 与场景六唯一的实现差异：本类有**两个** `@KafkaListener` 方法，
 * [onSlowEvent] 只监听 `slow-events`，[onOtherEvent] 只监听 `other-events`。
 * Spring Kafka 会为每个 `@KafkaListener` 方法各自创建一个独立的
 * `KafkaMessageListenerContainer`，进而各自创建一个独立的 `KafkaConsumer` 实例，
 * 并分别调用：
 * ```
 * consumer1.subscribe(Collections.singletonList("slow-events"), rebalanceListener)
 * consumer2.subscribe(Collections.singletonList("other-events"), rebalanceListener)
 * ```
 * 两个 `KafkaConsumer` 虽然使用相同的 `group.id`（因此是同一个 consumer group
 * 的两个成员，共享同一个 group coordinator），但各自的
 * `subscriptions.assignedPartitions()`（Kafka Client 内部用于 EAGER 协议
 * revoke 判断的集合，见 [MultiTopicRebalanceListener] 类注释的源码分析）
 * 天然只包含自己订阅的那一个 topic 的分区，彼此完全隔离。
 *
 * 预期结果（由源码可预判，将通过实测验证）：`slow-events` 对应的 consumer 触发
 * rebalance 时，只会 revoke/重新分配 `slow-events` 自己的 3 个分区；
 * `other-events` 对应的**另一个独立 consumer** 完全不受影响，持续正常消费，
 * 不应观察到它的分区被 revoke，也不应观察到它的消费出现空窗。
 */
@Component
@Profile("scenario7")
class SplitTopicRebalanceListener {

    private val log = LoggerFactory.getLogger(SplitTopicRebalanceListener::class.java)

    @KafkaListener(
        topics = ["\${app.kafka.topic}"],
        groupId = "\${spring.kafka.consumer.group-id}",
        id = "scenario7-slow-events-listener"
    )
    fun onSlowEvent(
        event: DemoEvent,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int,
        acknowledgment: Acknowledgment
    ) {
        log.info("[slow-events] 收到消息 id={} partition={} slow={}", event.id, partition, event.slow)

        if (event.slow) {
            log.warn(
                "[slow-events] 模拟慢处理开始：id={} 将阻塞 {}ms（超过 max.poll.interval.ms，预期触发该 consumer 的 rebalance）",
                event.id, SLOW_PROCESS_MS
            )
            Thread.sleep(SLOW_PROCESS_MS)
        } else {
            Thread.sleep(NORMAL_PROCESS_MS)
        }

        try {
            acknowledgment.acknowledge()
            log.info("[slow-events] 消息处理完成并已提交 offset id={}", event.id)
        } catch (ex: Exception) {
            log.warn("[slow-events] 提交 offset 失败，可能因 rebalance 导致该分区已被回收 id={}", event.id, ex)
        }
    }

    @KafkaListener(
        topics = ["\${app.kafka.other-topic:other-events}"],
        groupId = "\${spring.kafka.consumer.group-id}",
        id = "scenario7-other-events-listener"
    )
    fun onOtherEvent(
        event: DemoEvent,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int,
        acknowledgment: Acknowledgment
    ) {
        log.info("[other-events] 收到消息 id={} partition={}，正常处理", event.id, partition)
        Thread.sleep(NORMAL_PROCESS_MS)
        try {
            acknowledgment.acknowledge()
            log.info("[other-events] 消息处理完成并已提交 offset id={}", event.id)
        } catch (ex: Exception) {
            log.warn("[other-events] 提交 offset 失败，可能因 rebalance 导致该分区已被回收 id={}", event.id, ex)
        }
    }

    companion object {
        private const val SLOW_PROCESS_MS = 10_000L
        private const val NORMAL_PROCESS_MS = 100L
    }
}
