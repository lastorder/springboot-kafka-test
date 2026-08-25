package com.example.kafkarebalance.producer

import com.example.kafkarebalance.model.DemoEvent
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import java.time.Instant
import java.util.UUID

/**
 * 应用启动后自动发送一批演示事件到 Kafka。
 *
 * 约 1/4 的消息会被标记为 slow=true，消费端会对这些消息模拟"处理很慢"，
 * 从而使 poll 之间的间隔超过 max.poll.interval.ms，触发 consumer group rebalance。
 */
@Component
class DemoEventProducer(
    private val kafkaTemplate: KafkaTemplate<String, DemoEvent>,
    @Value("\${app.kafka.topic}") private val topic: String
) : ApplicationRunner {

    private val log = LoggerFactory.getLogger(DemoEventProducer::class.java)

    override fun run(args: ApplicationArguments) {
        Thread {
            // 等待 Kafka 连接与监听容器就绪，避免过早发送
            Thread.sleep(3_000)
            repeat(MESSAGE_COUNT) { index ->
                val slow = index % 4 == 0
                val event = DemoEvent(
                    id = UUID.randomUUID().toString(),
                    payload = "demo-event-$index",
                    slow = slow,
                    createdAt = Instant.now()
                )
                val key = (index % 3).toString()
                kafkaTemplate.send(topic, key, event)
                log.info(
                    "已发送消息 index={} id={} key={} slow={}",
                    index, event.id, key, slow
                )
                Thread.sleep(SEND_INTERVAL_MS)
            }
            log.info("全部 {} 条演示消息发送完毕", MESSAGE_COUNT)
        }.apply {
            name = "demo-event-producer"
            isDaemon = true
        }.start()
    }

    companion object {
        private const val MESSAGE_COUNT = 12
        private const val SEND_INTERVAL_MS = 200L
    }
}
