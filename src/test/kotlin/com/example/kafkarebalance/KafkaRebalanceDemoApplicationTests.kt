package com.example.kafkarebalance

import com.example.kafkarebalance.config.KafkaTopicConfig
import com.example.kafkarebalance.listener.SlowEventListener
import com.example.kafkarebalance.producer.DemoEventProducer
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.FilterType

/**
 * 仅验证 Spring 上下文能否正常加载，不依赖真实的 Kafka broker。
 *
 * 排除依赖 Kafka 的业务 Bean（producer/listener/topic 配置）。
 * 由于 Spring Boot 4 的 KafkaAutoConfiguration 仍会创建默认的
 * consumer/producer factory，测试期间可能会看到若干条连接 localhost:9092
 * 失败的 WARN 日志，属预期现象，不影响测试通过；
 * 真实的 Kafka 集成行为（生产、消费、rebalance）通过手动运行应用 + docker-compose 验证。
 */
@SpringBootTest
@ComponentScan(
    basePackages = ["com.example.kafkarebalance"],
    excludeFilters = [
        ComponentScan.Filter(
            type = FilterType.ASSIGNABLE_TYPE,
            classes = [DemoEventProducer::class, SlowEventListener::class, KafkaTopicConfig::class]
        )
    ]
)
class KafkaRebalanceDemoApplicationTests {

    @Test
    fun contextLoads() {
    }
}
