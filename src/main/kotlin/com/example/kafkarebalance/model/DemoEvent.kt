package com.example.kafkarebalance.model

/**
 * 演示用的事件模型，供四种 rebalance 场景共用。
 *
 * @param id           事件唯一标识
 * @param payload      事件内容
 * @param slow         是否模拟"单条处理很慢"（场景一使用，true 时消费端会人为阻塞较长时间）
 * @param retryTrigger 是否模拟"处理失败需要重试"（场景三使用，true 时消费端会一直失败直到重试耗尽）
 * @param flaky        是否模拟"下游临时不可用"（场景四使用，true 时消费端会先失败若干次，
 *                     待"下游恢复"后再成功处理，用于演示指数退避重试最终成功的场景）
 * @param createdAt    事件创建时间（ISO-8601 字符串，避免依赖额外的 Jackson JSR-310 模块）
 */
data class DemoEvent(
    val id: String,
    val payload: String,
    val slow: Boolean = false,
    val retryTrigger: Boolean = false,
    val flaky: Boolean = false,
    val createdAt: String
)
