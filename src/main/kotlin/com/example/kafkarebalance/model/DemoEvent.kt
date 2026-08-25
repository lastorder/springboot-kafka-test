package com.example.kafkarebalance.model

import java.time.Instant

/**
 * 演示用的事件模型。
 *
 * @param id        事件唯一标识
 * @param payload   事件内容
 * @param slow      是否模拟"慢处理"（true 时消费端会人为阻塞，触发 rebalance）
 * @param createdAt 事件创建时间
 */
data class DemoEvent(
    val id: String,
    val payload: String,
    val slow: Boolean,
    val createdAt: Instant
)
