package com.example.kafkarebalance.model

import java.time.Instant

/**
 * 演示用的事件模型，供三种 rebalance 场景共用。
 *
 * @param id           事件唯一标识
 * @param payload      事件内容
 * @param slow         是否模拟"单条处理很慢"（场景一使用，true 时消费端会人为阻塞较长时间）
 * @param retryTrigger 是否模拟"处理失败需要重试"（场景三使用，true 时消费端会抛出可重试异常）
 * @param createdAt    事件创建时间
 */
data class DemoEvent(
    val id: String,
    val payload: String,
    val slow: Boolean = false,
    val retryTrigger: Boolean = false,
    val createdAt: Instant
)
