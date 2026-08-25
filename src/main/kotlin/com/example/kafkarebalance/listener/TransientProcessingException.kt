package com.example.kafkarebalance.listener

/**
 * 场景三、场景四共用：模拟一次"可重试"的瞬时处理失败（例如临时下游依赖异常）。
 *
 * - 场景三：配合 [com.example.kafkarebalance.config.Scenario3ErrorHandlerConfig] 中注册的
 *   [org.springframework.kafka.listener.DefaultErrorHandler] + 固定间隔阻塞式 BackOff，
 *   用于演示"单次重试等待超过 max.poll.interval.ms 触发 rebalance"的场景。
 * - 场景四：配合 [com.example.kafkarebalance.config.Scenario4ErrorHandlerConfig] 中注册的
 *   使用指数退避 BackOff 的 DefaultErrorHandler，用于演示"下游临时不可用、
 *   重试若干次后恢复成功，且全程不触发 rebalance"的场景。
 */
class TransientProcessingException(message: String) : RuntimeException(message)
