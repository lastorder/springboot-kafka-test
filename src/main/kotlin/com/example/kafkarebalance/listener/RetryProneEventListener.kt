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
 * 场景三：监听 slow-events topic，模拟"整体配置合理，但阻塞式重试导致累计超时"。
 *
 * 每条消息正常处理耗时只有 200ms（10 条累计 2000ms，远小于
 * application-scenario3.yml 中 max.poll.interval.ms=8000ms，配置本身看起来完全合理）。
 *
 * 但当消息标记 retryTrigger=true 时，本方法会抛出 [TransientProcessingException]。
 * 该异常被 [com.example.kafkarebalance.config.Scenario3ErrorHandlerConfig] 中注册的
 * DefaultErrorHandler 捕获并按 FixedBackOff 阻塞式重试：每次重试前同步等待 9000ms，
 * 最多重试 3 次。
 *
 * 重要（已通过实测校正）：DefaultErrorHandler 在每次重试之间都会重新调用
 * KafkaConsumer#poll()，因此"多次重试的总耗时"并不会累加计入 max.poll.interval.ms；
 * 真正的风险点在于**单次**重试等待时间本身——只要这一次等待超过 max.poll.interval.ms，
 * 距离上一次成功 poll() 的间隔就已经超时，同样会触发 rebalance，
 * 即便平时"平均"配置看起来完全合理。
 */
@Component
@Profile("scenario3")
class RetryProneEventListener {

    private val log = LoggerFactory.getLogger(RetryProneEventListener::class.java)

    @KafkaListener(
        topics = ["\${app.kafka.topic}"],
        groupId = "\${spring.kafka.consumer.group-id}"
    )
    fun onMessage(
        event: DemoEvent,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int,
        acknowledgment: Acknowledgment
    ) {
        log.info(
            "收到消息 id={} partition={} retryTrigger={}",
            event.id, partition, event.retryTrigger
        )

        if (event.retryTrigger) {
            log.error(
                "模拟处理失败：id={}，将抛出可重试异常，触发 DefaultErrorHandler 的阻塞式重试（单次等待 9000ms，最多 3 次）",
                event.id
            )
            throw TransientProcessingException("模拟瞬时处理失败 id=${event.id}")
        }

        Thread.sleep(NORMAL_PROCESS_MS)

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
        private const val NORMAL_PROCESS_MS = 200L
    }
}
