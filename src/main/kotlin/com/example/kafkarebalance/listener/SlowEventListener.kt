package com.example.kafkarebalance.listener

import com.example.kafkarebalance.model.DemoEvent
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.stereotype.Component

/**
 * 监听 slow-events topic。
 *
 * 当收到 slow=true 的消息时，人为阻塞 10 秒来模拟"业务处理很慢"。
 * 由于 application.yml 中 max.poll.interval.ms 配置为 6000ms（小于 10s），
 * 消费者在两次 poll 之间的间隔会超过该阈值，broker 端的 group coordinator
 * 会判定该 consumer 已失联，从而触发 consumer group 的 rebalance。
 *
 * 注意：Thread.sleep 直接阻塞消费线程仅用于教学演示，
 * 生产代码中不应在监听方法里进行同步阻塞式的耗时操作。
 */
@Component
class SlowEventListener {

    private val log = LoggerFactory.getLogger(SlowEventListener::class.java)

    @KafkaListener(
        topics = ["\${app.kafka.topic}"],
        groupId = "\${spring.kafka.consumer.group-id}"
    )
    fun onMessage(
        event: DemoEvent,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int,
        acknowledgment: Acknowledgment
    ) {
        log.info("收到消息 id={} partition={} slow={}", event.id, partition, event.slow)

        if (event.slow) {
            log.warn(
                "模拟慢处理开始：id={} 将阻塞 {}ms（超过 max.poll.interval.ms=6000ms，预期触发 rebalance）",
                event.id, SLOW_PROCESS_MS
            )
            Thread.sleep(SLOW_PROCESS_MS)
        } else {
            Thread.sleep(NORMAL_PROCESS_MS)
        }

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
        private const val SLOW_PROCESS_MS = 10_000L
        private const val NORMAL_PROCESS_MS = 200L
    }
}
