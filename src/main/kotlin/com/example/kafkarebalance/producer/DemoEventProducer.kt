package com.example.kafkarebalance.producer

import com.example.kafkarebalance.model.DemoEvent
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.core.env.Environment
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import java.time.Instant
import java.util.UUID

/**
 * 应用启动后自动发送一批演示事件到 Kafka。
 *
 * 根据当前激活的 Spring Profile（scenario1~5）采用不同的
 * 发送策略，以配合各自监听器演示不同成因（或不触发）consumer group rebalance 的场景。
 */
@Component
class DemoEventProducer(
    private val kafkaTemplate: KafkaTemplate<String, DemoEvent>,
    private val environment: Environment,
    @Value("\${app.kafka.topic}") private val topic: String
) : ApplicationRunner {

    private val log = LoggerFactory.getLogger(DemoEventProducer::class.java)

    override fun run(args: ApplicationArguments) {
        val scenario = resolveActiveScenario()
        Thread {
            // 等待 Kafka 连接与监听容器就绪，避免过早发送
            Thread.sleep(3_000)
            when (scenario) {
                Scenario.SCENARIO2 -> sendScenario2Events()
                Scenario.SCENARIO3 -> sendScenario3Events()
                Scenario.SCENARIO4 -> sendScenario4Events()
                Scenario.SCENARIO5 -> sendScenario5Events()
                else -> sendScenario1Events()
            }
        }.apply {
            name = "demo-event-producer"
            isDaemon = true
        }.start()
    }

    /**
     * 场景一：12 条消息，约 1/4 标记为 slow=true，发送间隔 200ms，
     * key 按 index%3 轮询分散到 3 个分区。
     */
    private fun sendScenario1Events() {
        log.info("[scenario1] 开始发送 {} 条演示消息（约 1/4 标记 slow=true）", SCENARIO1_MESSAGE_COUNT)
        repeat(SCENARIO1_MESSAGE_COUNT) { index ->
            val slow = index % 4 == 0
            val event = newEvent(index, slow = slow)
            val key = (index % 3).toString()
            send(event, key)
            Thread.sleep(SCENARIO1_SEND_INTERVAL_MS)
        }
        log.info("[scenario1] 全部演示消息发送完毕")
    }

    /**
     * 场景二：15 条消息全部发到同一个 key（同一分区），几乎无间隔发送，
     * 确保它们在 consumer 首次 poll 之前就已全部写入 broker，
     * 从而一次 poll 能拉满 max.poll.records=10 条，触发累计耗时超时。
     */
    private fun sendScenario2Events() {
        log.info(
            "[scenario2] 开始向单一分区连续发送 {} 条消息，用于填满一次 poll 批次（max.poll.records=10）",
            SCENARIO2_MESSAGE_COUNT
        )
        repeat(SCENARIO2_MESSAGE_COUNT) { index ->
            val event = newEvent(index, slow = false)
            send(event, SCENARIO2_FIXED_KEY)
        }
        log.info("[scenario2] 全部演示消息发送完毕")
    }

    /**
     * 场景三：12 条消息发到同一个 key（同一分区），其中第 1 条和第 7 条
     * （对应两个可能的 poll 批次的首条）标记 retryTrigger=true，
     * 用于触发阻塞式重试。
     */
    private fun sendScenario3Events() {
        log.info(
            "[scenario3] 开始向单一分区发送 {} 条消息，其中 index={} 会触发一次可重试异常",
            SCENARIO3_MESSAGE_COUNT, SCENARIO3_RETRY_TRIGGER_INDICES
        )
        repeat(SCENARIO3_MESSAGE_COUNT) { index ->
            val retryTrigger = index in SCENARIO3_RETRY_TRIGGER_INDICES
            val event = newEvent(index, slow = false, retryTrigger = retryTrigger)
            send(event, SCENARIO2_FIXED_KEY)
            Thread.sleep(SCENARIO3_SEND_INTERVAL_MS)
        }
        log.info("[scenario3] 全部演示消息发送完毕")
    }

    private fun send(event: DemoEvent, key: String) {
        kafkaTemplate.send(topic, key, event)
        log.info(
            "已发送消息 id={} key={} slow={} retryTrigger={} flaky={}",
            event.id, key, event.slow, event.retryTrigger, event.flaky
        )
    }

    /**
     * 场景四：3 条消息发到同一个 key（同一分区），其中 index=1 标记 flaky=true，
     * 用于模拟"下游服务临时不可用"；其余消息为普通消息，用于验证该场景下
     * 正常消息不受影响、能够快速处理。
     */
    private fun sendScenario4Events() {
        log.info(
            "[scenario4] 开始向单一分区发送 {} 条消息，其中 index={} 会模拟下游临时不可用",
            SCENARIO4_MESSAGE_COUNT, SCENARIO4_FLAKY_INDEX
        )
        repeat(SCENARIO4_MESSAGE_COUNT) { index ->
            val flaky = index == SCENARIO4_FLAKY_INDEX
            val event = newEvent(index, slow = false, flaky = flaky)
            send(event, SCENARIO2_FIXED_KEY)
            Thread.sleep(SCENARIO1_SEND_INTERVAL_MS)
        }
        log.info("[scenario4] 全部演示消息发送完毕")
    }

    private fun newEvent(
        index: Int,
        slow: Boolean,
        retryTrigger: Boolean = false,
        flaky: Boolean = false
    ): DemoEvent =
        DemoEvent(
            id = UUID.randomUUID().toString(),
            payload = "demo-event-$index",
            slow = slow,
            retryTrigger = retryTrigger,
            flaky = flaky,
            createdAt = Instant.now().toString()
        )

    /**
     * 场景五：只发送 1 条消息，标记 flaky=true，专用于精确测量
     * "处理耗时 + 退避等待"之间的时间关系，不需要额外的干扰消息。
     */
    private fun sendScenario5Events() {
        log.info("[scenario5] 开始发送 1 条 flaky 消息，用于验证退避计时是否独立于处理耗时")
        val event = newEvent(0, slow = false, flaky = true)
        send(event, SCENARIO2_FIXED_KEY)
        log.info("[scenario5] 消息发送完毕")
    }

    private fun resolveActiveScenario(): Scenario {
        val activeProfiles = environment.activeProfiles
        return when {
            activeProfiles.contains("scenario2") -> Scenario.SCENARIO2
            activeProfiles.contains("scenario3") -> Scenario.SCENARIO3
            activeProfiles.contains("scenario4") -> Scenario.SCENARIO4
            activeProfiles.contains("scenario5") -> Scenario.SCENARIO5
            else -> Scenario.SCENARIO1
        }
    }

    private enum class Scenario { SCENARIO1, SCENARIO2, SCENARIO3, SCENARIO4, SCENARIO5 }

    companion object {
        private const val SCENARIO1_MESSAGE_COUNT = 12
        private const val SCENARIO1_SEND_INTERVAL_MS = 200L

        private const val SCENARIO2_MESSAGE_COUNT = 15
        private const val SCENARIO2_FIXED_KEY = "0"

        private const val SCENARIO3_MESSAGE_COUNT = 12
        private const val SCENARIO3_SEND_INTERVAL_MS = 50L
        private val SCENARIO3_RETRY_TRIGGER_INDICES = setOf(0, 6)

        private const val SCENARIO4_MESSAGE_COUNT = 3
        private const val SCENARIO4_FLAKY_INDEX = 1
    }
}
