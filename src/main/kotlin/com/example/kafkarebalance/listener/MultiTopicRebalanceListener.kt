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
 * 场景六：验证"同一个 consumer group 内的同一个 consumer 实例，同时消费多个 topic 时，
 * 其中一个 topic 的消息处理异常触发 rebalance，是否会影响另一个 topic 的消费"。
 *
 * 关键实现细节：本类**只有一个** `@KafkaListener` 方法，但 `topics` 属性同时列出
 * `slow-events` 和 `other-events` 两个 topic。Spring Kafka 的
 * `KafkaMessageListenerContainer` 会为这一个 `@KafkaListener` 方法创建
 * **一个** `KafkaConsumer` 实例，并调用
 * `consumer.subscribe(Arrays.asList("slow-events", "other-events"), rebalanceListener)`——
 * 也就是说这一个 consumer 真正地"同时订阅了两个 topic"，这与"每个 topic 各用一个
 * 独立 `@KafkaListener`/独立 consumer"是完全不同的场景（后者虽然共享 group.id，
 * 但各自的 Kafka `Consumer.subscription()` 只包含自己那一个 topic，
 * 不构成本场景要验证的"单个 consumer 跨多 topic 订阅"情形）。
 *
 * 关键背景（源码依据，详见 docs/rebalance-analysis.md 场景六章节）：Kafka 默认的
 * `partition.assignment.strategy=[RangeAssignor, CooperativeStickyAssignor]`，
 * `ConsumerCoordinator` 构造时会取所有配置 assignor 支持协议的**交集**并选择其中
 * 最高级的协议——`RangeAssignor` 只支持 `EAGER`，因此交集必然是 `[EAGER]`，
 * 即默认情况下使用的是 **EAGER 协议**。在 EAGER 协议下，
 * `ConsumerCoordinator#onJoinPrepare` 会无条件执行
 * `revokedPartitions.addAll(subscriptions.assignedPartitions())`——这里的
 * `subscriptions.assignedPartitions()` 是该 consumer **当前订阅的全部 topic**
 * 已分配的分区集合，而不仅仅是触发异常的那个 topic 的分区。因此预期：
 * `slow-events` 因慢消息触发 rebalance 时，`other-events` 的分区**也会**被一并
 * revoke、一并重新分配，即便 `other-events` 自身消费完全正常。
 */
@Component
@Profile("scenario6")
class MultiTopicRebalanceListener {

    private val log = LoggerFactory.getLogger(MultiTopicRebalanceListener::class.java)

    @KafkaListener(
        topics = ["\${app.kafka.topic}", "\${app.kafka.other-topic:other-events}"],
        groupId = "\${spring.kafka.consumer.group-id}"
    )
    fun onMessage(
        event: DemoEvent,
        @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int,
        acknowledgment: Acknowledgment
    ) {
        log.info("收到消息 id={} topic={} partition={} slow={}", event.id, topic, partition, event.slow)

        if (event.slow) {
            log.warn(
                "模拟慢处理开始：id={} topic={} 将阻塞 {}ms（超过 max.poll.interval.ms，预期触发 rebalance）",
                event.id, topic, SLOW_PROCESS_MS
            )
            Thread.sleep(SLOW_PROCESS_MS)
        } else {
            Thread.sleep(NORMAL_PROCESS_MS)
        }

        try {
            acknowledgment.acknowledge()
            log.info("消息处理完成并已提交 offset id={} topic={}", event.id, topic)
        } catch (ex: Exception) {
            log.warn("提交 offset 失败，可能因 rebalance 导致该分区已被回收 id={} topic={}", event.id, topic, ex)
        }
    }

    companion object {
        private const val SLOW_PROCESS_MS = 10_000L
        private const val NORMAL_PROCESS_MS = 100L
    }
}
