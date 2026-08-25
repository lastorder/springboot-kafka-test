package com.example.kafkarebalance.listener

/**
 * 场景三专用：模拟一次"可重试"的瞬时处理失败（例如临时下游依赖异常）。
 *
 * 配合 [com.example.kafkarebalance.config.Scenario3ErrorHandlerConfig] 中注册的
 * [org.springframework.kafka.listener.DefaultErrorHandler] + 阻塞式 BackOff，
 * 用于演示"重试等待耗时叠加导致超过 max.poll.interval.ms"的场景。
 */
class TransientProcessingException(message: String) : RuntimeException(message)
