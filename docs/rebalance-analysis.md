# Kafka Consumer Group Rebalance 场景日志分析报告

本报告基于 `docs/scenario1.log`、`docs/scenario2.log`、`docs/scenario3.log` 三次真实本地运行
（Spring Boot 4.0.4、spring-kafka 4.0.4、Kafka 4.1.2 broker、单节点 KRaft，通过 `docker compose`
本地启动）产生的日志，逐一分析每种场景下 rebalance 的触发过程，并总结对业务的潜在风险。

> 复现环境：单实例（只有 1 个 consumer）加入 `slow-consumer-group`。由于组内只有一个成员，
> 每次 rebalance 实际表现为"该成员被踢出 → 重新 JoinGroup → 重新拿回全部分区"，
> 不会像多实例场景那样把分区转移给别的存活实例。但触发条件、日志特征、以及对
> **消息处理与 offset 提交**的影响，与多实例场景完全一致，因此结论同样适用于生产环境的多实例部署。

---

## 场景一：单条消息处理过慢直接超时（`docs/scenario1.log`）

### 配置
- `max.poll.records=1`，`max.poll.interval.ms=6000`
- `SlowEventListener` 对 `slow=true` 的消息 `Thread.sleep(10_000)`

### 观察到的过程
1. `16:24:07.548` 消费者首次加入组，拿到全部 3 个分区（`slow-events-0/1/2`）。
2. `16:24:10.599` 起，`DemoEventProducer` 开始发送 12 条消息（约 1/4 标记 `slow=true`）。
3. `16:24:16.649` 首次出现：
   ```
   WARN  ConsumerCoordinator - consumer poll timeout has expired. This means the time
   between subsequent calls to poll() was longer than the configured max.poll.interval.ms...
   INFO  ConsumerCoordinator - Member ... sending LeaveGroup request ... due to consumer poll timeout has expired.
   ```
4. `16:24:20.649` 容器层面确认：`partitions revoked: [slow-events-0, slow-events-1, slow-events-2]`。
5. 几乎同时（`16:24:20.667`）该消费者又重新 `Adding newly assigned partitions`——因为组内只有它一个成员，
   revoke 后立刻重新拿回全部分区。
6. **整个日志窗口内，这个"revoke → 重新 assign"的循环重复了 18 次**（`grep -c "partitions revoked"` = 18）。
7. 关键现象：**12 条消息中只有 1 条（`0863b692-...`，`slow=false`）成功走到"处理完成并提交 offset"**；
   其余消息尤其是被卡在 `slow=true` 上的那条（`472138ad-...`）反复触发下列日志且从未成功提交：
   ```
   WARN  SlowEventListener - 提交 offset 失败，可能因 rebalance 导致该分区已被回收 id=472138ad-...
   ```

### 根因

`max.poll.interval.ms` 描述的是"两次调用 `KafkaConsumer#poll()` 之间允许的最大间隔"。
`SlowEventListener` 在处理消息期间用 `Thread.sleep()` 同步阻塞了消费线程，这个线程正是
负责调用 `poll()` 的线程。一旦阻塞时间超过阈值，broker 端的 group coordinator 就会认为
该 consumer"卡死"，将其踢出组。由于本例中只有 1 个 consumer，被踢出后它会立刻重新
`JoinGroup` 并再次拿到全部分区——但**分区被 revoke 的那一刻，之前正在处理、尚未提交的
offset 全部作废**，重新分配后消费者会从**上一次成功提交的 offset**重新开始拉取，
导致同一条消息被反复重新投递、反复处理、反复失败提交，形成事实上的死循环。

---

## 场景二：批量数量过大导致累计超时（`docs/scenario2.log`）

### 配置
- `max.poll.records=10`，`max.poll.interval.ms=6000`（与场景一相同的阈值，仅批量大小不同）
- `BatchSlowEventListener` 对每条消息固定处理 800ms（单条并不慢）

### 观察到的过程
1. `16:27:43.953` 消费者加入组，拿到全部 3 个分区。
2. `16:27:47.003` `DemoEventProducer` 已把 15 条消息全部发送到同一分区（`key=0`），几乎无发送间隔。
3. `16:27:53.087`：
   ```
   WARN  ConsumerCoordinator - consumer poll timeout has expired ...
   ```
4. `16:27:55.152` `partitions revoked`，随即重新拿回全部分区。
5. **整个运行期间只发生了 2 次 rebalance**（早期批次较大时各触发一次），随后趋于稳定；
   **15 条消息全部成功处理并提交 offset**（`grep -c "消息处理完成并已提交"` = 15）。

### 根因

`max.poll.records=10` 允许一次 `poll()` 拉取多达 10 条消息；Spring Kafka 容器会在
**同一个消费线程、同一次 poll 循环内逐条同步处理**这些消息（这是默认行为，无需显式
开启"批量监听"模式）。虽然单条消息处理只有 800ms、看起来完全不慢，但
`10 条 × 800ms = 8000ms`，已经超过 `max.poll.interval.ms=6000ms`，因此在批次数量较大、
消息挤压在同一次 poll 里时同样会触发 rebalance——**这与场景一的成因完全不同：
场景一是"单条本身就慢"，场景二是"单条不慢，但乘以批量数量后总和超标"**。

日志中只发生 2 次而非持续发生，是因为该场景**没有"处理失败/无法提交"的消息**——
一旦某次批次因为凑巧拉到多条消息导致超时触发一次 rebalance，重新分配后 consumer
会从新的（更靠后的）已提交 offset 继续消费，此后随着消息逐渐被处理完、新到消息量减少，
每次 poll 拉到的消息数自然下降，不再稳定触发超时。

### 补充分析：第一次 rebalance 前后，offset 到底提交到了哪里？

一个容易被忽略但很关键的问题是：**触发 rebalance 那一刻，之前已经处理过的消息，
offset 究竟有没有提交成功？** 通过逐行核对 `docs/scenario2.log` 中第一次 rebalance
（`16:27:55.152 partitions revoked`）前后的 `Committing:` / `Committed offset` 日志，
可以得到非常明确的答案：**部分提交成功，部分处于"已发起但未确认"的中间状态**。

第一次 poll 拉到 10 条消息（offset 1~10），`BatchSlowEventListener` 按顺序逐条处理、
每条处理完立即调用 `ack.acknowledge()`：

```
16:27:47.023 Received: 10 records                         <- 一次 poll 拉到 10 条（offset 1~10）
16:27:47.845 Committed offset 1 for partition slow-events-2   <- 已确认提交
16:27:48.659 Committed offset 2 for partition slow-events-2   <- 已确认提交
16:27:49.475 Committed offset 3 for partition slow-events-2   <- 已确认提交
16:27:50.291 Committed offset 4 for partition slow-events-2   <- 已确认提交
16:27:51.099 Committed offset 5 for partition slow-events-2   <- 已确认提交
16:27:51.911 Committed offset 6 for partition slow-events-2   <- 已确认提交
16:27:52.723 Committed offset 7 for partition slow-events-2   <- 已确认提交（最后一次收到确认）
16:27:53.530 Committing: {slow-events-2=OffsetAndMetadata{offset=8, ...}}   <- 只有"发起"日志
16:27:54.343 Committing: {slow-events-2=OffsetAndMetadata{offset=9, ...}}   <- 只有"发起"日志
16:27:55.151 Committing: {slow-events-2=OffsetAndMetadata{offset=10, ...}}  <- 只有"发起"日志
16:27:55.152 partitions revoked: [slow-events-0, slow-events-1, slow-events-2]   <- rebalance 发生
```

关键观察：
- **offset 1~7 明确提交成功**（每一条都有对应的 `Committed offset N` 回执日志），
  这 7 条消息的 offset 已经安全写入 `__consumer_offsets`，不会被重复消费。
- **offset 8、9、10 只看到 `Committing:`（客户端发起提交请求）、没有看到对应的
  `Committed`（收到 broker 确认）**——这三次提交请求几乎是贴着 poll 超时/发起
  LeaveGroup 的时间点发出的，此时消费者可能已经失去了这些分区的所有权，
  提交请求大概率会失败或被 broker 拒绝（`CommitFailedException` 或静默失败，
  取决于失败发生的具体阶段），日志中确实没有出现与之对应的确认记录。

rebalance 完成、消费者重新拿回分区后的日志印证了这一点：

```
16:27:55.161 Received: 8 records                          <- 重新拿到分区后，从上次真正提交成功的位置继续拉取
16:27:55.967 Committing: {slow-events-2=OffsetAndMetadata{offset=8, ...}}
16:27:55.975 Committed offset 8 for partition slow-events-2   <- 这次才真正确认成功
16:27:55.975 消息处理完成并已提交 offset id=fe121eca-...       <- 对应 offset=7 那条消息重新被处理
```

`Received: 8 records`（而不是剩下的 5 条）说明 broker 端真正生效的已提交 offset
就是 7（下一条待消费 offset 是 8），也就是说 **offset 8、9、10 对应的三条消息
在 rebalance 后被重新投递、重新处理了一遍**，尽管它们在 rebalance 之前实际上已经
被 `BatchSlowEventListener` 完整处理过一次（业务逻辑已经执行完毕，只是提交 offset
的请求没有来得及被确认）。

**结论**：场景二并不是"要么全部提交成功、要么全部作废重来"的非黑即白局面，而是
**在 rebalance 发生的临界时刻，总会有一小段"已处理但提交状态不确定"的消息**——
这批消息的业务逻辑对外部系统产生的副作用（如果有的话）已经真实发生了一次，
rebalance 后还会再发生一次。相比场景一里"同一条消息死循环重试"的极端情况，
场景二给出的是一个更具普遍性、更贴近真实生产环境的证据：**任何一次 rebalance，
无论其成因是什么、无论批次内是否存在"毒消息"，其临界时刻附近的若干条消息
都天然处于重复处理的高风险窗口内**，这进一步强化了下文"消费逻辑必须幂等"
这一结论的必要性。

---

## 场景三：整体配置合理，但阻塞式重试的单次等待超标（`docs/scenario3.log`）

### 配置（含一次实测后的修正）
- `max.poll.records=10`，`max.poll.interval.ms=8000`
- `RetryProneEventListener` 对正常消息处理 200ms（10 条累计仅 2000ms，远低于阈值，"看起来合理"）
- 对标记 `retryTrigger=true` 的消息抛出 `TransientProcessingException`，由
  `Scenario3ErrorHandlerConfig` 中的 `DefaultErrorHandler + FixedBackOff` 捕获重试

#### 一次重要的设计修正（诚实记录）

最初的设计假设是：`DefaultErrorHandler` 的重试是"完全不释放线程的连续阻塞"，因此
"多次重试的总耗时"会累加到 `max.poll.interval.ms` 里。**但实测结果推翻了这个假设**：

日志证据（`FixedBackOff(3000ms, maxAttempts=3)` 的初始配置下）：
```
16:30:45.802 Received: 1 records
16:30:45.806 第 1 次投递失败，将进行阻塞式重试等待 3000ms
16:30:48.825 Received: 10 records     <-- 注意：这里重新调用了 poll() 并拿到结果
16:30:48.826 第 2 次投递失败...
16:30:51.844 Received: 10 records     <-- 又一次 poll()
16:30:51.845 第 3 次投递失败...
```
可以清楚看到，**`DefaultErrorHandler` 在每次重试之间都会重新调用 `KafkaConsumer#poll()`**
（哪怕本地已经有数据在队列里），这会重置"距离上次成功 poll 的时间"这个计时器。
因此只要**单次**重试等待时间本身小于 `max.poll.interval.ms`，无论重试多少次、
总耗时多长，都不会触发 rebalance——这与最初的假设相反，也是本次实测最有价值的发现之一。

**修正后的配置**：把 `FixedBackOff` 的单次等待时间从 3000ms 调整为 **9000ms**
（超过 `max.poll.interval.ms=8000ms`），复现出真实的 rebalance。

### 观察到的过程（修正后）
1. `16:35:38.334` 第 1 次投递失败，开始 9000ms 阻塞等待。
2. `16:35:46.363`（约 8 秒后）：
   ```
   WARN  ConsumerCoordinator - consumer poll timeout has expired ...
   ```
3. `16:35:47.349` `partitions revoked`，随后重新拿回全部分区。
4. 消费者恢复后立即进行**第 2 次重试**（`16:35:47.384`），同样等待 9000ms，
   再次于 `16:35:55.384` 触发 poll 超时 → 再次 revoke → 再次重新分配。
5. 这个"重试等待 9s → 超时 → revoke → 重新分配 → 再次重试"的循环，对**同一条毒消息**
   重复了 4 次（1 次首次尝试 + 3 次重试），每一次都触发了一次 rebalance，
   直到 `FixedBackOff` 的重试次数耗尽：
   ```
   ERROR DefaultErrorHandler - Backoff FixedBackOffExecution[interval=9000, currentAttempts=4, maxAttempts=3] exhausted for slow-events-2@0
   ```
6. 该 topic 里共有 2 条"毒消息"（index=0 和 index=6），**共触发 6 次 rebalance**；
   其余 10 条正常消息全部成功提交 offset。重试耗尽后，`DefaultErrorHandler`
   跳过该消息、推进 offset，消费恢复正常。

### 根因

真正的风险点不是"多次重试总耗时累加"，而是：**只要单次阻塞等待（无论是重试 backoff、
还是任何形式的同步阻塞）本身超过了 `max.poll.interval.ms`，就会立即触发 rebalance**。
这意味着即便你的 `max.poll.records` 和 `max.poll.interval.ms` 配置得再"保守"、
平均处理耗时看起来再健康，只要引入了**一次等待时间较长的阻塞式重试**（例如遇到下游依赖
瞬时抖动、配置了较长的 backoff 想"避免立刻冲击下游"），就可能因为这一次等待触发 rebalance。

---

## 综合结论：Rebalance 对业务操作的潜在风险

### 1. 消息重复处理（At-Least-Once 语义被放大，甚至可能演变为死循环）

三个场景共同验证的核心风险：**分区被 revoke 时，正在处理但尚未提交的消息的 offset
不会被保留**。重新分配分区（无论分给自己还是别的实例）后，消费都会从
**上一次成功提交的 offset** 重新开始，导致：
- 已经处理过、但因 rebalance 而没能来得及提交 offset 的消息，会被**重复投递、重复处理**。
- 场景一中，如果业务处理不是幂等的（比如"扣减库存"“发送短信"“调用支付"），
  会造成**重复扣减、重复发短信、重复扣款**等严重后果。
- 更极端地，场景一实测中出现了**同一条消息被反复投递、反复处理、反复提交失败**的
  近似死循环现象（18 次 rebalance 中只成功提交了 1 条消息）——如果这类"毒消息"
  没有幂等保护 + 没有最终跳过机制，会导致该分区的消费**长期停滞不前**，
  后续消息全部被阻塞在其之后，造成消费延迟无限扩大。
- 场景二的日志进一步证明，即便没有"毒消息"、批次整体处理完全正常，**rebalance
  发生的临界时刻附近仍然会有若干条"已处理但提交状态不确定"的消息**（详见该场景的
  补充分析），这些消息在 rebalance 后会被重新投递、重复处理一遍——说明重复处理风险
  并非只存在于"处理异常"的极端场景，而是**任意一次 rebalance 都会天然产生的通用现象**。

**业务建议**：所有 Kafka 消费逻辑必须做到幂等（例如基于消息唯一 ID 做去重表/状态机判断），
否则 rebalance 会把"处理慢"的问题放大成"数据重复/数据错误"的更严重问题。

### 2. 消费延迟骤增、消息堆积

每次 rebalance 期间，参与 rebalance 的所有分区在完成重新分配前都**无法被任何消费者消费**
（即便是被分配回同一个消费者，也有一段"revoke → 重新 JoinGroup/SyncGroup → 重新 assign"
的空窗期）。场景一、三中可以看到从触发超时到重新拿到分区，通常有 1~2 秒的空窗；
如果是多分区、多实例的生产环境，加上更多消费者参与 rebalance（stop-the-world 式的
`eager` 分配策略下，所有分区在 rebalance 期间对所有消费者都不可用），空窗期会更长。
这会直接表现为：
- 消息处理延迟（lag）在监控图表上出现突刺甚至持续增长；
- 下游依赖这些事件的业务（如库存扣减通知、订单状态流转）出现明显的处理滞后；
- 如果 rebalance 像场景一那样反复发生，Lag 可能持续增长而不收敛，最终触发告警甚至
  影响 SLA。

### 3. 生产者/下游看到的"重复副作用"与幂等性缺失的组合风险

结合第 1 点，如果消费者在处理消息时会对外部系统产生副作用（调用支付网关、
发送消息通知、写入下游数据库），rebalance 导致的重复投递会让这些副作用
**不可控地重复执行**。这在金融、库存等强一致性要求的场景中是高优先级风险，
必须通过"消费端幂等 + 生产端去重/精确一次语义（如事务性 outbox、Kafka 事务）"
来缓解。

### 4. "看似正常"的配置也可能突然触发 rebalance（场景三揭示的隐蔽风险）

场景三最有价值的发现是：**平均耗时/常规配置完全正常，不代表系统对"偶发的单次长耗时
阻塞"免疫**。现实中很多团队会在错误处理中加入"重试 + 固定/指数 backoff"，
如果这个 backoff 的**单次**等待时间没有对照 `max.poll.interval.ms` 做过校验，
一旦下游依赖出现哪怕一次抖动导致触发一次较长的重试等待，就可能意外触发 rebalance，
进而引发上述第 1、2 点的连锁反应。这类问题往往在压测/日常运行中不会暴露，
只有在下游真正抖动、真正触发重试路径时才会出现，因此具有较强的隐蔽性和"生产环境突发故障"特征。

### 5. 频繁 Rebalance 本身对 Kafka 集群的额外开销

即便每次 rebalance 都能"自愈"（如本次单实例场景），频繁的 JoinGroup/SyncGroup/
LeaveGroup 请求本身也会给 group coordinator（broker）带来额外负载；在多实例、
多消费组的生产集群中，如果同一时间有多个消费组因为类似原因反复 rebalance，
可能对 broker 的处理能力造成整体性影响，是需要监控和告警的对象
（可监控 `kafka.consumer:type=consumer-coordinator-metrics` 下的
`rebalance-total`、`rebalance-rate-per-hour` 等指标）。

---

## 缓解建议汇总

| 风险成因 | 对应场景 | 建议缓解措施 |
|---|---|---|
| 单条消息处理耗时不可控（依赖第三方/大计算量） | 场景一 | 异步化处理 + 尽快 ack；或将耗时任务转移出监听线程（如投递到内部队列/线程池，主线程快速返回） |
| `max.poll.records` 设置过大 | 场景二 | 结合"最坏情况下单条处理耗时 × max.poll.records"反推 `max.poll.interval.ms`，或改用较保守的 `max.poll.records` |
| 阻塞式重试的单次等待时间设置不当 | 场景三 | 重试 backoff 的**单次**最大等待时间必须显著小于 `max.poll.interval.ms`；耗时较长的重试建议改为异步重试（如 Spring Kafka 的 Retry Topic / 非阻塞重试机制），避免占用主消费线程 |
| 消息重复处理 | 全部场景 | 消费逻辑必须幂等；关键业务配合去重表/状态机/唯一约束 |
| Rebalance 期间消费空窗 | 全部场景 | 监控 consumer lag 与 rebalance 频率指标；评估是否可切换到 `CooperativeStickyAssignor` 等增量再均衡策略以减少"stop-the-world"影响 |

---

## 附：三份原始日志与运行参数对照

| 文件 | Profile | 关键参数 | 观察到的 rebalance 次数 | 说明 |
|---|---|---|---|---|
| `docs/scenario1.log` | `scenario1` | `max.poll.records=1`，`max.poll.interval.ms=6000` | 18 次 | 单条 slow=true 消息处理 10s 直接超时；出现"毒消息"死循环现象 |
| `docs/scenario2.log` | `scenario2` | `max.poll.records=10`，`max.poll.interval.ms=6000` | 2 次 | 单条 800ms 不慢，批量累计 8000ms 超时；全部 15 条消息最终成功处理，但首次 rebalance 边界处有 3 条消息（offset 8~10）被重复投递处理，详见场景二补充分析 |
| `docs/scenario3.log` | `scenario3` | `max.poll.records=10`，`max.poll.interval.ms=8000`，`FixedBackOff(9000ms, 3次)` | 6 次 | 2 条"毒消息" × 最多 4 次尝试，每次单独等待即超时；其余 10 条消息正常提交 |

（日志文件较大，均已完整保留在 `docs/` 目录，关键片段的时间戳可用于交叉核对本报告中的分析。）
