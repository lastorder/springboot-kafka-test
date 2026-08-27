# Kafka Consumer Group Rebalance 场景日志分析报告

本报告基于 `docs/scenario1.log`、`docs/scenario2.log`、`docs/scenario3.log`、`docs/scenario4.log`、
`docs/scenario5.log`、`docs/scenario6.log`、`docs/scenario7.log` 七次真实本地运行
（Spring Boot 4.0.4、spring-kafka 4.0.4、Kafka 4.1.2 broker、单节点 KRaft，
通过 `docker compose` 本地启动）产生的日志，逐一分析每种场景下 rebalance 的触发过程
（或不触发的原因），并总结对业务的潜在风险。

> 复现环境：场景一~五均为单实例（只有 1 个 consumer）加入 `slow-consumer-group`。
> 由于组内只有一个成员，每次 rebalance 实际表现为"该成员被踢出 → 重新 JoinGroup →
> 重新拿回全部分区"，不会像多实例场景那样把分区转移给别的存活实例。但触发条件、
> 日志特征、以及对**消息处理与 offset 提交**的影响，与多实例场景完全一致，因此结论
> 同样适用于生产环境的多实例部署。场景六、七在单实例基础上引入了**第二个 topic**
> 和**同一 consumer group 内的第二个 consumer 成员**，用于验证同一 consumer group
> 消费多 topic 时的相互影响，详见场景六、七章节。

> 七个场景中，场景一、二、三演示的是**会触发 rebalance** 的三种不同成因；
> 场景四是**不触发 rebalance** 的正面案例——通过合理配置指数退避的单次等待上限，
> 即便下游服务临时不可用、需要重试很多次、总耗时很长，也能安全完成重试而不影响消费组稳定性；
> 场景五专门用于回答一个更细节的计时问题："如果处理已经花费了一部分时间才失败，
> 退避等待是否会扣除这部分已耗费的时间？"（答案见场景五章节）；
> 场景六回答"同一个 consumer group 消费多个 topic 时，一个 topic 的异常是否会
> 影响另一个 topic"这一架构层面的问题（答案：会，详见场景六章节）；
> 场景七则进一步验证"把多 topic 订阅拆分成多个独立的 `@KafkaListener` 是否能避免
> 这种影响"（答案：能大幅缓解，但控制面仍有极小概率的短暂"陪同参与"，详见场景七章节）。

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

## 场景四：下游服务临时不可用，指数退避最终成功且不触发 rebalance（`docs/scenario4.log`）

场景三证明了"单次退避等待超过 `max.poll.interval.ms` 就会触发 rebalance"，
场景四则是这一结论的**正面印证**：只要把退避的**单次等待上限**始终控制在
`max.poll.interval.ms` 以内，哪怕总共重试很多次、总耗时很长（生产环境甚至可以长达
15 分钟），也完全不会触发 rebalance。这是应对"下游服务临时不可用，但预期会在
一段时间内恢复"这类场景的推荐做法。

### 配置

- `max.poll.records=1`，`max.poll.interval.ms=6000`（本地实测用的压缩版阈值）
- `Scenario4ErrorHandlerConfig` 注册 `DefaultErrorHandler` + `ExponentialBackOffWithMaxRetries`：
  - `initialInterval=500ms`，`multiplier=2.0`，`maxInterval=3000ms`，`maxRetries=8`
  - 各次退避等待依次为：500 → 1000 → 2000 → 3000 → 3000 → 3000 → 3000 → 3000（ms），
    单次上限 3000ms，相对 `max.poll.interval.ms=6000ms` 留有 2 倍安全余量
- `FlakyDownstreamEventListener` 模拟"下游临时不可用"：标记 `flaky=true` 的消息在
  **前 4 次**尝试都会抛出可重试异常，**第 5 次**尝试起视为"下游已恢复"，正常处理并提交 offset

> 说明：本地实测使用的是**时间压缩版**参数（秒级，总预算约 18.5 秒），用于在几十秒内
> 完整复现整个链路；生产环境应使用的真实参数（分钟级，总预算约 15 分钟）见本节末尾的
> "生产环境最优配置"。两者的参数比例关系完全一致，只是量纲不同。

### 观察到的过程（`docs/scenario4.log`）

```
17:38:12.396 收到消息 ... 这是第 1 次尝试（模拟下游服务临时不可用）
17:38:12.396 模拟下游仍不可用：第 1 次尝试失败，将触发指数退避重试
17:38:12.398 第 1 次投递失败，将进行指数退避重试

17:38:12.909 收到消息 ... 这是第 2 次尝试     <- 距上次尝试约 513ms（对应 initialInterval=500ms）
17:38:12.909 模拟下游仍不可用：第 2 次尝试失败

17:38:13.955 收到消息 ... 这是第 3 次尝试     <- 距上次尝试约 1046ms（对应 500*2=1000ms）
17:38:13.955 模拟下游仍不可用：第 3 次尝试失败

17:38:16.025 收到消息 ... 这是第 4 次尝试     <- 距上次尝试约 2070ms（对应 500*4=2000ms）
17:38:16.025 模拟下游仍不可用：第 4 次尝试失败

17:38:19.140 收到消息 ... 这是第 5 次尝试     <- 距上次尝试约 3115ms（对应 maxInterval 封顶 3000ms）
17:38:19.140 模拟下游已恢复：第 5 次尝试将正常处理
17:38:19.255 消息处理完成并已提交 offset id=3cc8708b-...
```

四次重试的等待间隔依次约为 0.51s → 1.05s → 2.07s → 3.12s，与设计的
500/1000/2000/3000ms 完全吻合（指数增长，第 4 次起触达 `maxInterval=3000ms` 封顶）。

**全程零 rebalance**：`grep -c "partitions revoked\|poll timeout has expired" docs/scenario4.log`
结果为 **0**。日志中消费者的 `generation`（组成员代际号）自始至终都是 `1`，
从未发生变化——这是"该 consumer 从未被踢出过组"的直接证据。心跳线程照常独立工作：

```
17:38:57.491 Received successful Heartbeat response
17:39:00.493 Received successful Heartbeat response
17:39:03.494 Received successful Heartbeat response
...（每 3 秒一次心跳，全程正常，不受重试阻塞影响）
```

最终 3 条消息（1 条 flaky + 2 条普通消息）全部成功提交 offset
（`grep -c "消息处理完成并已提交" docs/scenario4.log` = 3），业务完全正确完成，
没有任何消息因为 rebalance 被重复投递或被跳过。

### 根因

原理与场景三完全对称：`DefaultErrorHandler` 在每次重试之间都会重新调用
`KafkaConsumer#poll()`，这个动作本身就会重置"距离上次成功 poll 的时间"计时器。
因此触发 rebalance 的唯一条件是**单次**等待时间超过 `max.poll.interval.ms`，
与总共重试了多少次、累计等待了多长时间无关。`ExponentialBackOffWithMaxRetries`
通过 `maxInterval` 参数天然提供了"单次等待上限"的封顶机制——只要这个封顶值
小于 `max.poll.interval.ms`（并留出安全余量），指数退避可以放心地重试任意多次、
持续任意长时间，而不会有触发 rebalance 的风险。

### 生产环境最优配置

若要真实满足"最多等待 15 分钟，期间多次重试，最终成功"的需求，建议配置：

| 参数 | 建议值 | 说明 |
|---|---|---|
| `initialInterval` | 5,000 ms（5s） | 首次重试等待时间，不宜过短（避免对刚故障的下游造成瞬时重试风暴）也不宜过长 |
| `multiplier` | 2.0 | 标准指数退避倍率 |
| `maxInterval` | 60,000 ms（60s） | **单次**退避等待的上限——这是决定是否触发 rebalance 的关键参数 |
| `maxRetries` | 18 | 由 `ExponentialBackOffWithMaxRetries(maxRetries)` 自动反推总耗时：500ms 起指数增长并在 60s 封顶，18 次总计约 915s ≈ **15.25 分钟**，贴近"最多等待 15 分钟"的预算 |
| `max.poll.interval.ms` | 90,000 ms（90s） | 至少为 `maxInterval` 的 1.5～2 倍安全余量；此处取 60s 的 1.5 倍 |
| `max.poll.records` | 建议保持较小（如 1～5） | 避免与"批量导致累计超时"（场景二）的风险叠加 |
| `session.timeout.ms` / `heartbeat.interval.ms` | 按 Kafka 集群默认或适度调大即可（如 10000ms / 3000ms） | 心跳线程独立于重试阻塞的消费线程，不受重试等待影响，无需为此特意调整 |

**参数选择的通用方法**：
1. 先确定业务可接受的总重试预算 `T`（本例 15 分钟）和下游典型恢复时间估计。
2. 选择 `initialInterval` 和 `multiplier`（通常 5s 起、2 倍是较常见且稳妥的组合）。
3. 选择 `maxInterval`（单次上限），通常取该预算下"愿意接受的最大单次等待间隔"，
   例如 30s～120s 之间，同时必须满足 `maxInterval < max.poll.interval.ms`（建议至少
   1.5 倍安全余量，避免时钟抖动、GC 停顿等因素导致实际等待略超预期）。
4. 用等比数列公式反推 `maxRetries`：在指数增长阶段耗时之和，加上封顶后
   `(maxRetries - 增长阶段次数) × maxInterval`，使总和贴近预算 `T`。
   `ExponentialBackOffWithMaxRetries` 会根据 `maxRetries` 自动计算并锁定
   `maxElapsedTime`，无需（也不允许）手动设置。
5. 将 `max.poll.interval.ms` 设置为 `maxInterval` 的 1.5～2 倍，作为最终防线。

---

## 场景五：处理耗时是否会被退避等待时间"扣除"？（`docs/scenario5.log`）

### 提出的问题

场景三、四都涉及"重试等待时间"，但都没有回答一个更细节的问题：

> **如果一次 poll 的消息处理已经花了 N 秒（比如调用下游、等待超时耗费了这些时间），
> 然后抛出异常触发退避等待 M 秒，那么距离下一次重新调用 poll() 的实际间隔，
> 是 M 秒，还是 (M-N) 秒？**

这个问题在评估"退避 + 处理耗时会不会一起把 `max.poll.interval.ms` 顶爆"时非常关键——
如果退避时间会自动扣除已经消耗的处理时间，那么总耗时上限就是 `max(单次退避时间, 处理时间)`；
如果不会扣除，总耗时上限就是"处理时间 + 完整退避时间"两者相加。场景一、二、三、四
现有的日志都无法回答这个问题，因为这些场景中"异常抛出前的处理耗时"要么是 0
（立即抛异常），要么虽然有耗时但没有精确到毫秒级去对比退避等待的实际时长。
因此专门设计了本场景来做一次单变量对照实验。

### 源码依据（先看代码，再看实测是否吻合）

阅读 spring-kafka 4.0.4 的 `DefaultErrorHandler` 调用链路源码：

```java
// org.springframework.kafka.listener.FailedRecordTracker#recovered
long nextBackOff = failedRecord.getBackOffExecution().nextBackOff();
if (nextBackOff != BackOffExecution.STOP) {
    this.backOffHandler.onNextBackOff(container, exception, nextBackOff);
    return false;
}

// org.springframework.kafka.listener.DefaultBackOffHandler#onNextBackOff
@Override
public void onNextBackOff(MessageListenerContainer container, Exception exception, long nextBackOff) {
    if (container == null) {
        Thread.sleep(nextBackOff);
    }
    else {
        ListenerUtils.stoppableSleep(container, nextBackOff);
    }
}
```

`nextBackOff` 的值直接来自 `BackOffExecution#nextBackOff()`——也就是 `FixedBackOff`/
`ExponentialBackOff` 配置的间隔本身，**代码中没有任何地方读取"本次处理已经消耗了多久"
并做减法**。据此可以从源码层面预判：退避等待时间与处理耗时无关，是从**抛出异常的那一刻**
开始独立计时的完整时长。本场景通过实测验证这个源码层面的判断是否成立。

### 配置

- `Scenario5ErrorHandlerConfig` 注册 `FixedBackOff(10000ms, maxAttempts=3)`（固定退避 10 秒）
- `BackoffTimingEventListener` 对 flaky 消息前 2 次尝试：先 `Thread.sleep(4000ms)`
  模拟"处理耗费了 4 秒才失败"，再抛出异常；第 3 次尝试视为成功
- `application-scenario5.yml` 中 `max.poll.interval.ms=20000`（20 秒），刻意设置得
  足够宽松（远大于"处理 4s + 退避 10s = 14s"的最坏情况），确保本次实验只关注
  "退避计时是否独立于处理耗时"这一个变量，不会被 rebalance 干扰
- 每条日志都记录 `System.currentTimeMillis()` 的精确时间戳（`startEpochMs`/
  `throwEpochMs`/`endEpochMs`），避免依赖日志打印本身的时刻误差，直接从时间戳算出准确间隔

### 实测结果（`docs/scenario5.log`）

```
09:30:12.654 收到消息 ... 第 1 次尝试，开始处理，startEpochMs=1787707812654
09:30:16.659 模拟处理失败：第 1 次尝试在耗费 4000ms 处理后抛出异常，throwEpochMs=1787707816659（实际耗时=4005ms）
09:30:16.668 第 1 次投递失败，接下来将退避等待 10000ms

09:30:26.731 收到消息 ... 第 2 次尝试，开始处理，startEpochMs=1787707826731
09:30:30.734 模拟处理失败：第 2 次尝试在耗费 4000ms 处理后抛出异常，throwEpochMs=1787707830734（实际耗时=4003ms）
09:30:30.735 第 2 次投递失败，接下来将退避等待 10000ms

09:30:40.749 收到消息 ... 第 3 次尝试，开始处理，startEpochMs=1787707840749
09:30:40.749 第 3 次尝试成功：endEpochMs=1787707840749（距离本次处理开始耗时=0ms）
09:30:40.758 消息处理完成并已提交 offset
```

用精确的 epoch 毫秒时间戳计算实际间隔：

| 区间 | 计算 | 结果 |
|---|---|---|
| 第 1 次抛出异常 → 第 2 次开始处理 | `1787707826731 - 1787707816659` | **10,072 ms** |
| 第 2 次抛出异常 → 第 3 次开始处理 | `1787707840749 - 1787707830734` | **10,015 ms** |
| 第 1 次开始处理 → 第 2 次开始处理（处理 4s + 退避 10s 的总耗时） | `1787707826731 - 1787707812654` | **14,077 ms** |
| 第 2 次开始处理 → 第 3 次开始处理 | `1787707840749 - 1787707826731` | **14,018 ms** |

### 结论：退避时间**不会**扣除已消耗的处理时间

两次"抛出异常 → 下一次开始处理"的间隔均约为 **10,000ms**（10072ms、10015ms，
误差在几十毫秒的调度抖动范围内），与配置的 `FixedBackOff(10000ms)` 完全吻合，
**丝毫没有体现出"扣除已经耗费的 4 秒处理时间"的迹象**（如果会扣除，应该约为
10000-4000=6000ms 左右，但实测远高于这个数值）。

同时，两次"开始处理 → 下一次开始处理"的总间隔均约为 **14,000ms**（14077ms、14018ms），
精确等于"处理耗时(4000ms) + 退避等待(10000ms)"之和，进一步印证了这一点。

**回答用户的问题**：如果一次处理已经花了 N 秒才失败，退避配置的等待时间是 M 秒，
那么距离下一次重新调用 poll() 的实际间隔是**完整的 M 秒**（从抛出异常那一刻开始计时），
**而不是 (M-N) 秒**。换句话说，"处理耗时"和"退避等待时间"是**顺序累加**的关系，
不存在互相抵扣。这与前面阅读 `DefaultBackOffHandler` 源码得出的预判完全一致。

### 这对评估 rebalance 风险意味着什么

结合场景三、四的结论（"决定是否触发 rebalance 的是单次退避等待时间是否超过
`max.poll.interval.ms`"），场景五进一步明确了完整的计算公式：

```
单次重试循环的总耗时 = 本次处理耗时（含抛异常前的耗时） + 本次退避等待时间
```

如果这个总耗时超过了 `max.poll.interval.ms`，就会触发 rebalance——**必须把"处理耗时"
和"退避等待时间"两者相加一起对照 `max.poll.interval.ms`，而不能只看退避时间本身**。
这意味着：即便退避的 `maxInterval` 本身小于 `max.poll.interval.ms`，如果处理逻辑
本身在抛出异常前也消耗了不可忽略的时间（比如调用下游超时耗费了数秒才失败），
两者相加仍然可能超过阈值，触发原本"看起来不该发生"的 rebalance。这是设计
"生产环境最优配置"（场景四章节）时必须额外考虑的安全余量来源之一。

---

## 场景六：同一 consumer group 消费多个 topic 时，一个 topic 的异常是否会影响另一个 topic？（`docs/scenario6.log`）

### 提出的问题

生产环境中很常见的部署方式是：一个应用用**同一个 consumer group** 同时消费多个 topic
（例如 `order-events` 和 `notification-events`）。如果其中一个 topic（比如
`order-events`）因为消息处理异常触发了 rebalance，**同一个 consumer group 里
正在正常消费的另一个 topic（`notification-events`）会不会也被牵连、出现短暂的
消费中断？**

### 实验设计

关键实现细节（详见 [`MultiTopicRebalanceListener.kt`](../src/main/kotlin/com/example/kafkarebalance/listener/MultiTopicRebalanceListener.kt)
的类注释）：本场景**只用一个** `@KafkaListener` 方法，但 `topics` 属性同时列出
`slow-events` 和 `other-events` 两个 topic 名。这一点很重要——如果分别用两个
`@KafkaListener` 方法各自监听一个 topic（哪怕 `groupId` 相同），Spring Kafka 会
为每个方法创建**各自独立的 `KafkaConsumer`**，每个 consumer 的 `subscribe()` 调用
只包含它自己的那一个 topic，不会构成"单个 consumer 同时订阅多个 topic"的场景。
只有像本场景这样让同一个 `@KafkaListener`（对应同一个 `KafkaMessageListenerContainer`
和同一个 `KafkaConsumer` 实例）在 `topics` 属性里列出多个 topic 名，才会调用
`consumer.subscribe(Arrays.asList("slow-events", "other-events"), rebalanceListener)`，
让一个 consumer 真正同时订阅两个 topic——这也正是本场景要验证的、生产环境中
"一个消费者应用用同一个 group 订阅多个 topic"的真实情形。

### 配置与运行方式

- `application-scenario6.yml` 复用场景一的触发阈值：`max.poll.interval.ms=6000`，
  `max.poll.records=1`
- `DemoEventProducer` 启动后：
  1. 后台线程每 500ms 向 `other-events` 发送 1 条正常消息，持续 30 秒，
     模拟"另一个 topic 的正常业务流量"
  2. 5 秒后向 `slow-events` 发送 1 条 `slow=true` 的消息，触发处理阻塞 10 秒，
     超过 `max.poll.interval.ms=6000ms`，从而触发该 consumer 被踢出组

### 实测结果（`docs/scenario6.log`）

启动阶段，一个 consumer 同时拿到两个 topic 的全部分区：

```
Adding newly assigned partitions: [other-events-0, other-events-1, other-events-2, slow-events-0, slow-events-1, slow-events-2]
```

`other-events` 开始以约 500ms 间隔稳定消费（`11:44:03.219` ~ `11:44:07.658`），
与此同时 `slow-events` 收到那条 `slow=true` 消息并开始阻塞处理
（`11:44:08.041`）。**从这一刻起，`other-events` 的消费也随之停止**：

```
11:44:07.658  收到消息 ... topic=other-events （最后一条，rebalance 前）
11:44:08.041  收到消息 ... topic=slow-events slow=true （开始阻塞 10s）
11:44:13.860  WARN  consumer poll timeout has expired
11:44:13.861  INFO  Member ... sending LeaveGroup request ... due to consumer poll timeout has expired
11:44:18.053  INFO  partitions revoked: [other-events-0, other-events-1, other-events-2, slow-events-0, slow-events-1, slow-events-2]
11:44:18.071  INFO  Adding newly assigned partitions: [other-events-0, other-events-1, other-events-2, slow-events-0, slow-events-1, slow-events-2]
11:44:18.082  收到消息 ... topic=other-events （恢复消费，第一条）
```

**`other-events` 的消费中断了约 10.4 秒（`18.082 - 07.658`）**，而这段时间里
`other-events` 本身没有任何异常、没有任何处理耗时问题——它的中断完全是由
`slow-events` 那条无关的慢消息引起的。而且 `partitions revoked` 日志明确列出了
**两个 topic 的全部 6 个分区**，而不仅仅是 `slow-events` 的 3 个分区。

这个"消费中断 → 全部 6 分区一起 revoke → 全部 6 分区一起重新分配"的循环
在实测中重复了 **15 次**（`grep -c "partitions revoked" docs/scenario6.log` = 15）
——因为该 `slow=true` 消息始终无法成功提交 offset（与场景一相同的"毒消息"机制），
每一轮的两个 topic 6 个分区都会被一起 revoke、一起重新分配，逐字节完全一致：

```bash
$ grep "partitions revoked" docs/scenario6.log | sed -E 's/.*partitions revoked: //' | sort -u
[other-events-0, other-events-1, other-events-2, slow-events-0, slow-events-1, slow-events-2]
```

（15 次 revoke 的分区集合**完全相同**，没有任何一次只 revoke 了部分 topic 的分区。）

### 结论：会影响，而且是"全部 topic 一起被牵连"

**同一个 consumer group 内的同一个 consumer 同时订阅多个 topic 时，只要其中一个
topic 的消息处理触发了 rebalance，该 consumer 当前订阅的其余所有 topic 的分区
也会被一并 revoke、一并重新分配——即便这些 topic 自身的消费完全正常，也会经历
一次"消费中断 + 重新加入组"的过程。**

### 源码依据：为什么会这样？（Kafka Client 关键设计）

阅读 Kafka Client 4.1.2 的 `org.apache.kafka.clients.consumer.internals.ConsumerCoordinator`
源码，可以定位到两处决定性的设计：

**1. Rebalance 协议的选择（构造函数中）**

```java
// ConsumerCoordinator 构造函数
if (!assignors.isEmpty()) {
    List<RebalanceProtocol> supportedProtocols = new ArrayList<>(assignors.get(0).supportedProtocols());
    for (ConsumerPartitionAssignor assignor : assignors) {
        supportedProtocols.retainAll(assignor.supportedProtocols());   // 取交集
    }
    Collections.sort(supportedProtocols);
    protocol = supportedProtocols.get(supportedProtocols.size() - 1); // 取交集中最高级的协议
}
```

`RebalanceProtocol` 枚举定义为 `EAGER(0), COOPERATIVE(1)`（数值越大越"高级"）。
`partition.assignment.strategy` 默认值是 `[RangeAssignor, CooperativeStickyAssignor]`
（已通过实际运行 `ConsumerConfig.configDef()` 核实，见下方默认值附录）。而
`RangeAssignor` 并未重写 `supportedProtocols()`，因此使用接口默认值
`Collections.singletonList(RebalanceProtocol.EAGER)`——只支持 `EAGER`。
`CooperativeStickyAssignor` 支持 `[COOPERATIVE, EAGER]`。两者取交集结果是
`[EAGER]`，因此**默认情况下选定的 rebalance 协议就是 EAGER**（这不是巧合，
而是只要 assignor 列表里包含任何一个只支持 EAGER 的分配器，交集就必然退化为 EAGER；
`RangeAssignor` 排在默认列表首位，天然把整体协议锁定为 EAGER）。

**2. EAGER 协议下 `onJoinPrepare` 的无条件全量 revoke**

```java
// ConsumerCoordinator#onJoinPrepare
switch (protocol) {
    case EAGER:
        // revoke all partitions
        revokedPartitions.addAll(subscriptions.assignedPartitions());
        exception = rebalanceListenerInvoker.invokePartitionsRevoked(revokedPartitions);
        subscriptions.assignFromSubscribed(Collections.emptySet());
        break;

    case COOPERATIVE:
        // only revoke those partitions that are not in the subscription anymore.
        Set<TopicPartition> ownedPartitions = new HashSet<>(subscriptions.assignedPartitions());
        revokedPartitions.addAll(ownedPartitions.stream()
            .filter(tp -> !subscriptions.subscription().contains(tp.topic()))
            .collect(Collectors.toSet()));
        ...
}
```

这里的 `subscriptions.assignedPartitions()` 是该 **consumer 实例**（不是某个 topic）
当前已分配的**全部分区**——只要这个 consumer 同时订阅了 `slow-events` 和
`other-events`，这个集合就同时包含两个 topic 的分区。`EAGER` 分支的逻辑是
"不管三七二十一，先把手上所有分区全部吐出来"，因此触发条件只需要"这个 consumer
需要重新加入组"，而不关心"是哪个 topic 导致了这次重新加入"——这就是本场景实测
中"两个 topic 的分区总是一起被 revoke"的根本原因。

`COOPERATIVE` 分支则完全不同：只 revoke "已拥有但不再属于当前订阅"的分区
（`!subscriptions.subscription().contains(tp.topic())`）。如果两个 topic 都仍在
订阅列表里、且分配算法（如 `CooperativeStickyAssignor`）判断当前分配依然合理，
这个 revoke 集合可以是空的——也就是说在 `COOPERATIVE` 协议下，一次"因为某个
topic 处理慢导致的重新加入组"未必会导致*任何* topic 的分区被 revoke（取决于
分配算法的计算结果），或者即便发生 revoke，也大概率只涉及需要重新平衡的那部分
分区，而不是无条件牵连全部订阅的 topic。

### 配置分析：哪些配置项会影响这个行为？

| 配置项 | 作用位置 | 对本问题的影响 |
|---|---|---|
| `spring.kafka.consumer.properties.partition.assignment.strategy` | Consumer | **决定性因素**。默认 `[RangeAssignor, CooperativeStickyAssignor]` → 协议交集为 `EAGER` → 触发"全部 topic 一起 revoke"。若显式设置为 `org.apache.kafka.clients.consumer.CooperativeStickyAssignor`（只保留这一个，且不与任何仅支持 EAGER 的分配器混用），协议交集将变为 `[COOPERATIVE, EAGER]` 排序后取最高级即 `COOPERATIVE`，可以避免/减少无关 topic 被牵连 |
| `spring.kafka.consumer.properties.group.protocol` | Consumer | Kafka 2.8+ 引入的新一代消费者组协议（KIP-848，`classic` 或 `consumer`）。默认 `classic`（即本文档全部实验采用的传统协议栈）。设置为 `consumer` 会启用全新的服务端驱动的增量再均衡协议，从架构上就是增量分配、不会有"整体 EAGER revoke"的概念，但需要 broker 版本支持且是较新的特性，生产环境启用前需要充分验证 |
| `max.poll.interval.ms` / 处理耗时 / 重试退避时间 | Consumer | 不影响"是否会牵连其它 topic"，只影响"多久会触发一次这样的 rebalance"（详见前面场景一~五及"通用结论"章节） |
| Topic 是否共用同一个 `@KafkaListener`（即是否属于同一个 `KafkaConsumer` 实例的订阅集合） | 应用层设计 | 如果两个 topic 分别用**不同**的 `@KafkaListener` 方法监听（即便 `groupId` 相同），则它们是同一 consumer group 里的**两个独立 consumer 成员**，各自只订阅自己的 topic。这种情况下，一个 consumer 因处理慢被踢出组，只会导致**它自己订阅的那个 topic** 的分区被 revoke 并重新分配给组内其他能正常工作的 consumer（如果有），**不会**直接导致另一个 topic 的 consumer 也发生 revoke——因为它们是两个独立的 `KafkaConsumer.subscribe()` 调用，不共享 `subscriptions.assignedPartitions()` 集合。这是规避本场景问题的一种应用层设计方案（见下方"如何缓解"） |

### 如何缓解"一个 topic 的问题影响其它 topic"

结合上面的源码与配置分析，有两种思路（**方案 1 已通过场景七实测验证有效**，
详见下一章节）：

1. **应用层拆分**：如果多个 topic 的业务重要性/稳定性要求不同，避免让同一个
   `@KafkaListener`（同一个 consumer 实例）同时订阅多个 topic；改为每个 topic
   使用独立的 `@KafkaListener` 方法（即便共享同一个 `groupId`），这样它们在
   Kafka 协议层面就是彼此独立的订阅关系，一个 topic 的消费者被踢出组不会直接
   波及另一个 topic 的消费者的**分区分配**（虽然仍共享同一个 consumer group 的
   group coordinator，在极少数情况下——当另一个 topic 的 consumer 恰好是该
   consumer group 的 leader 时——仍会经历一次耗时可忽略不计的"陪同参与"，
   详见场景七）。
2. **切换到 COOPERATIVE 协议**：将 `partition.assignment.strategy` 配置为
   `org.apache.kafka.clients.consumer.CooperativeStickyAssignor`（且不要同时配置
   仅支持 EAGER 的分配器，否则协议交集仍会退化为 EAGER），利用增量再均衡的特性，
   减少"一个 consumer 需要重新加入组"时对其已拥有、且依然合理的其它 topic
   分区的影响。这个改动对本项目其它场景（一~五）演示的"是否会触发 rebalance"
   结论没有影响——`COOPERATIVE` 协议下，判断"是否需要重新加入组"的触发条件
   （`max.poll.interval.ms` 超时）完全不变，改变的只是"重新加入组时具体
   revoke 哪些分区"这一环节。

---

## 场景七：拆分成两个独立的 `@KafkaListener` 后，是否就不受影响了？（`docs/scenario7.log`）

### 提出的问题

场景六证明了"一个多 topic 的 `@KafkaListener`（即一个 consumer 同时订阅多个 topic）"
会让 rebalance 牵连全部订阅的 topic。一个自然的追问是：**如果把这个多 topic
`@KafkaListener` 拆分成两个各自独立的 `@KafkaListener`（每个只监听一个 topic，
但仍共享同一个 `groupId`），是不是就能避免这种牵连？**

### 实验设计

[`SplitTopicRebalanceListener.kt`](../src/main/kotlin/com/example/kafkarebalance/listener/SplitTopicRebalanceListener.kt)
与场景六的 [`MultiTopicRebalanceListener.kt`](../src/main/kotlin/com/example/kafkarebalance/listener/MultiTopicRebalanceListener.kt)
的唯一差异：把原来的一个方法拆成 `onSlowEvent`（只监听 `slow-events`）和
`onOtherEvent`（只监听 `other-events`）两个方法，`groupId` 保持相同。
`application-scenario7.yml` 与场景六完全一致的 `max.poll.interval.ms` 等配置，
`DemoEventProducer` 也复用与场景六完全相同的发送逻辑（`sendMultiTopicEvents`），
确保这是一次严格意义上的单变量对照实验。

Spring Kafka 会为这两个 `@KafkaListener` 方法各自创建独立的
`KafkaMessageListenerContainer` 和独立的 `KafkaConsumer` 实例，分别调用：
```
consumer1.subscribe(Collections.singletonList("slow-events"), rebalanceListener)
consumer2.subscribe(Collections.singletonList("other-events"), rebalanceListener)
```
两个 consumer 虽然共享同一个 `group.id`（因此是**同一个 consumer group 的
两个成员**，共享同一个 group coordinator），但各自的
`subscriptions.assignedPartitions()` 天然只包含自己的那个 topic。

### 实测结果（`docs/scenario7.log`）：会独立 revoke，但仍有一次"陪同参与"

先看好消息：`grep "partitions revoked"` 的结果集合只有两种，**从未出现过
两个 topic 混在一起的情况**：

```bash
$ grep "partitions revoked" docs/scenario7.log | sed -E 's/.*partitions revoked: //' | sort -u
[other-events-0, other-events-1, other-events-2]
[slow-events-0, slow-events-1, slow-events-2]
```

`slow-events` 的 consumer（`consumer-slow-consumer-group-1`）因慢消息被踢出组，
触发 rebalance，这部分与场景六表现完全一致（同一条"毒消息"导致反复 rebalance，
本次实测捕获了 7 次）。

但意外的是：**`other-events` 的 consumer（`consumer-slow-consumer-group-2`）
也经历了 revoke/重新分配**（本次实测捕获了 13 次！比 `slow-events` 自己还多），
日志显示原因是：

```
Request joining group due to: group is already rebalancing
```

深入日志发现，`consumer-slow-consumer-group-2`（也就是 `other-events` 的 consumer）
恰好是这个 consumer group 的 **leader**（Kafka 消费者组协议规定，组内某一个成员
会被指定为 leader，由它负责读取全部成员的订阅信息、计算分区分配方案）：

```
Received successful JoinGroup response: JoinGroupResponseData(..., leader='consumer-slow-consumer-group-2-...', ...)
```

**只要 group 内*任意*成员需要重新加入组（本场景中是 `slow-events` 的 consumer
因超时被踢出），整个 group 就会进入"rebalancing"状态，此时 leader 成员即使
自己没有任何问题，也必须参与这一轮 JoinGroup/SyncGroup 协议、重新计算并确认
分区分配方案**，因此也会经历一次自己的 `onJoinPrepare`（revoke 自己的分区）
→ 重新 JoinGroup → 重新计算分配 → `onJoinComplete`（拿回分区）流程。

### 关键区别：虽然也 revoke，但耗时可以忽略不计，且 offset 完全连续

这正是场景七与场景六最本质的区别所在。用精确时间戳比对：

```
12:14:35.700  [other-events] 消息处理完成并已提交 offset id=... (offset=25)
12:14:35.701  Revoke previously assigned partitions [other-events-0, other-events-1, other-events-2]
12:14:35.701  partitions revoked: [other-events-0, other-events-1, other-events-2]
12:14:35.702  (Re-)joining group ...
12:14:35.706  Successfully joined group with generation 2
12:14:35.706  Finished assignment for group at generation 2: {..., Assignment(partitions=[other-events-0, other-events-1, other-events-2])}
12:14:35.710  Successfully synced group in generation 2
12:14:35.712  Found no committed offset for partition other-events-0
12:14:35.713  Setting offset for partition other-events-2 to the committed offset ... offset=25   <- 精确衔接上次提交的 offset，未丢失/未重置
12:14:35.717  partitions assigned: [other-events-0, other-events-1, other-events-2]
12:14:36.095  [other-events] 收到消息 ...（下一条正常消息，继续消费）
```

**这一次完整的 revoke → 重新分配 → 恢复消费的耗时仅约 16 毫秒**
（`35.701` 到 `35.717`），offset 从上次提交的 25 精确衔接，没有任何数据丢失或重复的窗口。
对全部 `other-events` 消息的接收时间戳做逐条间隔分析，最大间隔仅为 **520ms**
（对应生产者约 500ms 的发送间隔本身，属于正常发送节奏，而非 rebalance 造成的额外延迟）：

```
$ python3 分析 other-events 全部 60 条消息的接收时间戳
最大相邻间隔：520ms（与生产者 500ms 发送间隔一致，无异常延迟）
```

而场景六中，同一个 `other-events` 的消费中断是 **10.4 秒**——两者相差约 **650 倍**。

### 结论：拆分后基本不受影响，但不是"完全零感知"

**回答用户的问题：把一个多 topic `@KafkaListener` 拆分成两个各自独立的
`@KafkaListener`（仍共享同一个 `groupId`），可以让一个 topic 的处理异常
**不再直接牵连**另一个 topic 的分区分配（不会像场景六那样把两个 topic
的全部分区混在一起 revoke），这是本次实测最重要的确认。**

但需要澄清一个更精确的结论，避免"完全没有任何影响"这种过度简化的说法：

1. **两个独立 consumer 依然共享同一个 consumer group**，因此当组内任何一个成员
   （即便是完全健康的那个）恰好被选为 **group leader**，它也会在组内其它
   成员触发 rebalance 时被要求"陪同参与"一次 JoinGroup/SyncGroup 协议往返，
   经历一次自己的 revoke → 重新分配。
2. 但这次"陪同参与"的 revoke 和场景六里那种因为**自己被判定超时踢出**而发生的
   revoke，在**性质和后果上完全不同**：leader 的陪同参与几乎是瞬时的（本次实测
   ~16ms），offset 精确衔接不丢失，消费吞吐几乎不受影响；而场景六里的那种
   revoke 对应的是"这个 consumer 已经失联了 `max.poll.interval.ms` 那么久"，
   需要走完整的 session timeout 检测、LeaveGroup、等待其它成员重新 JoinGroup
   的完整周期，耗时是秒级的。
3. 如果 `other-events` 的 consumer **不巧不是 leader**（leader 由哪个成员
   担任是不确定的，取决于哪个成员的 JoinGroup 请求先被 coordinator 处理），
   它可能完全不会经历任何 revoke（因为非 leader 成员只需要发送/接收 JoinGroup、
   SyncGroup 请求，不需要计算分配方案；无论是否为 leader，非 leader 成员的
   revoke 行为在 EAGER 协议下都遵循同样的"重新加入组即 revoke 自己的全部分区"
   规则，因此不论是否为 leader，只要该成员的 consumer 需要重新走一遍
   JoinGroup 流程，都会有一次 revoke——区别只在于"leader 因为要为全组计算
   分配方案而必然参与这一轮协议"与"非 leader 是否需要参与，取决于自己是否
   也在这一时刻恰好需要重新加入组"）。

综合来看：**拆分成独立的 `@KafkaListener` 是有效且推荐的缓解手段**——它把
"数据面"的影响（分区数据长时间不可用、offset 提交状态不确定、消息重复处理
的风险窗口）降到了几乎可以忽略的程度；但"控制面"层面（同一个 consumer group
内的 JoinGroup/SyncGroup 协议往返）仍然是共享的，无法做到绝对零感知。
如果需要**完全的故障隔离**（包括控制面），需要把不同重要性的 topic 拆分到
**不同的 consumer group**（这样会产生独立的 group coordinator 协调周期，
彼此的 JoinGroup/SyncGroup 完全独立，但代价是无法再共享同一份 offset
提交视图/同一套消费位点管理，需要根据实际业务需求权衡）。

---

## 通用结论：两次 poll 之间的间隔一旦超过 max.poll.interval.ms，就会触发 rebalance

综合七个场景的实测数据，可以归纳出一条贯穿全部场景、能够互相印证的**通用规则**：

> **只要消费线程连续两次调用 `KafkaConsumer#poll()` 之间实际经过的时间超过了
> `max.poll.interval.ms` 配置的阈值，就会触发一次 consumer group rebalance；
> 反之，只要每一次 poll 到下一次 poll 之间的间隔始终不超过该阈值，
> 无论期间发生了什么（单条处理慢、批量数量大、抛异常重试、退避等待等），
> 都不会触发 rebalance。**

这条规则在本项目的每个场景中都得到了独立验证，逐一核对如下：

| 场景 | 触发/不触发 | 两次 poll 间隔与 max.poll.interval.ms 的关系 | 日志证据 |
|---|---|---|---|
| 场景一 | 触发（18 次） | 单条消息处理 10s，一次 poll 只拉 1 条，下一次 poll 前必然等待 10s > 6s | `docs/scenario1.log`：处理开始到 `poll timeout has expired` 恰好间隔 6s 左右 |
| 场景二 | 触发（2 次） | 10 条 × 800ms = 8000ms > 6000ms，一次 poll 拉满 10 条时下一次 poll 前的处理耗时超标 | `docs/scenario2.log`：`Received: 10 records` 到下次 `Received` 之间间隔超过 6s 时触发 |
| 场景三 | 触发（6 次） | 单次退避等待 9000ms > 8000ms，退避期间不调用 poll，等待结束后才重新 poll，间隔本身已超标 | `docs/scenario3.log`：每次退避开始到下次 `Received: N records` 间隔约 9s |
| 场景四 | **不触发**（0 次） | 单次退避等待最高 3000ms（封顶）< 6000ms，即使总共重试 4 次也每次都独立小于阈值 | `docs/scenario4.log`：`generation` 全程为 1，从未变化 |
| 场景五 | 不触发（本场景 max.poll.interval.ms=20000 特意放宽） | 处理耗时(4s) + 退避(10s) = 14s < 20s，因此本场景本身也不会触发；但若换成 `max.poll.interval.ms=12000` 就会触发（14s > 12s） | `docs/scenario5.log`：可推算若阈值设为 12000ms 则必然超时 |
| 场景六 | 触发（15 次，同场景一的机制） | 触发原因与场景一相同（单条消息处理 10s > 6s），但本场景验证的是"触发后波及的范围"而非"触发条件本身" | `docs/scenario6.log`：`slow-events` 单条消息导致超时，`other-events` 随之一起被 revoke |
| 场景七 | `slow-events` 触发（7 次），`other-events` 因 leader 陪同参与也发生 revoke（13 次，但耗时仅约 16ms） | 触发条件与场景六完全相同，验证的是"拆分成独立 `@KafkaListener` 后是否还会互相影响" | `docs/scenario7.log`：两个 topic 的 revoke 集合从未混在一起；`other-events` 的 revoke→重新分配全程约 16ms，offset 精确衔接不丢失 |

需要特别强调三个容易被误解的细节（均已通过实测确认）：

1. **"两次 poll 之间的间隔"指的是一次 `poll()` 返回、到消费线程处理完这批记录、
   再次调用下一次 `poll()` 为止的全部耗时**——包括正常业务处理、批量内逐条处理的总和、
   以及 `DefaultErrorHandler` 抛出异常后的退避等待（退避期间不会调用 `poll()`，
   见场景三、四的源码分析）。**唯一的例外**是场景三、四揭示的机制：如果配置了
   重试且重试次数 > 1，`DefaultErrorHandler` 会在每一次退避等待结束后立即调用一次
   `poll()`（哪怕本地已有数据），这次 `poll()` 调用本身会重置计时器——所以"总重试次数"
   不会累加计入超时判断，真正起作用的是**每一段独立的 poll 间隔**（可能是"一次退避等待"，
   也可能是"一次退避等待+若干次处理"，取决于该次 poll 拿到多少条记录）。
2. 场景五进一步明确：**处理耗时和退避等待时间是顺序相加、不会互相抵扣的**，
   因此对照阈值时必须用"本段总耗时"（处理 + 退避），而非只看退避配置的数值本身。
3. 场景六进一步明确：**触发 rebalance 的条件（"这段 poll 间隔是否超过阈值"）与
   "触发后会影响哪些 topic"是两个独立的问题**。前者只取决于消费线程本身的行为，
   与订阅了几个 topic 无关；后者则取决于 rebalance 协议——默认 EAGER 协议下，
   一旦触发就会牵连该 consumer 当前订阅的**全部** topic，不区分"是谁导致的"。

这条通用规则可以作为诊断生产环境 rebalance 问题的第一步：只要能够从日志中定位到
"哪一段 poll 到 poll 之间的间隔超过了 `max.poll.interval.ms`"，就能确定 rebalance
的直接触发原因，再结合本文档六个场景的成因分类，进一步定位到具体是"单条慢"、
"批量大"、"重试退避超标"，还是其他导致该间隔过长的业务逻辑；如果消费者同时订阅了
多个 topic，还需要额外评估场景六揭示的"影响范围"问题。

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
场景六进一步证明，**这个空窗期不仅限于触发问题的那个 topic**：默认 EAGER 协议下，
同一个 consumer 订阅的其它 topic（哪怕自身完全健康）也会被一起纳入这段空窗期
（实测中 `other-events` 出现了约 10.4 秒的消费中断，纯粹是被 `slow-events` 的
慢消息"连坐"）。这会直接表现为：
- 消息处理延迟（lag）在监控图表上出现突刺甚至持续增长，且可能是**多个 topic 同时**出现；
- 下游依赖这些事件的业务（如库存扣减通知、订单状态流转）出现明显的处理滞后；
- 如果 rebalance 像场景一/六那样反复发生，Lag 可能持续增长而不收敛，最终触发告警甚至
  影响 SLA，并且告警可能来自表面上"毫无问题"的另一个 topic，增加排查难度。

### 3. 生产者/下游看到的"重复副作用"与幂等性缺失的组合风险

结合第 1 点，如果消费者在处理消息时会对外部系统产生副作用（调用支付网关、
发送消息通知、写入下游数据库），rebalance 导致的重复投递会让这些副作用
**不可控地重复执行**。这在金融、库存等强一致性要求的场景中是高优先级风险，
必须通过"消费端幂等 + 生产端去重/精确一次语义（如事务性 outbox、Kafka 事务）"
来缓解。

### 4. "看似正常"的配置也可能突然触发 rebalance（场景三揭示的隐蔽风险，场景四给出正解）

场景三最有价值的发现是：**平均耗时/常规配置完全正常，不代表系统对"偶发的单次长耗时
阻塞"免疫**。现实中很多团队会在错误处理中加入"重试 + 固定/指数 backoff"，
如果这个 backoff 的**单次**等待时间没有对照 `max.poll.interval.ms` 做过校验，
一旦下游依赖出现哪怕一次抖动导致触发一次较长的重试等待，就可能意外触发 rebalance，
进而引发上述第 1、2 点的连锁反应。这类问题往往在压测/日常运行中不会暴露，
只有在下游真正抖动、真正触发重试路径时才会出现，因此具有较强的隐蔽性和"生产环境突发故障"特征。

场景四证明了这个问题是**完全可以规避**的：只要确保退避的**单次等待上限**
（如 `ExponentialBackOffWithMaxRetries` 的 `maxInterval`）始终小于
`max.poll.interval.ms`（并留出安全余量），哪怕重试总时长长达 15 分钟，
也可以做到全程零 rebalance、消费组稳定、最终成功处理消息。这对于"下游有计划内
维护窗口"或"下游偶发抖动但通常会在几分钟到十几分钟内自愈"的场景是理想的应对策略。

### 5. 频繁 Rebalance 本身对 Kafka 集群的额外开销

即便每次 rebalance 都能"自愈"（如本次单实例场景），频繁的 JoinGroup/SyncGroup/
LeaveGroup 请求本身也会给 group coordinator（broker）带来额外负载；在多实例、
多消费组的生产集群中，如果同一时间有多个消费组因为类似原因反复 rebalance，
可能对 broker 的处理能力造成整体性影响，是需要监控和告警的对象
（可监控 `kafka.consumer:type=consumer-coordinator-metrics` 下的
`rebalance-total`、`rebalance-rate-per-hour` 等指标）。

### 6. "一荣俱损"的爆炸半径：一个 topic 的问题拖累同一 consumer 订阅的所有 topic（场景六），拆分独立 consumer 可大幅缓解但非绝对隔离（场景七）

场景六揭示了一个架构层面、容易被忽视的风险："同一个 consumer group 用同一个
`KafkaConsumer` 同时订阅多个 topic"这种常见的应用设计方式，会让原本互不相关的
多个业务（多个 topic）在故障隔离层面被**耦合在一起**。默认 EAGER 协议下，
只要其中一个 topic（哪怕业务重要性很低、只是个日志/审计类的次要 topic）出现
处理异常触发 rebalance，其它所有被同一个 consumer 订阅的 topic（哪怕是核心
交易链路的 topic）都会被一起 revoke、经历同样的消费中断。这意味着：
- 故障排查时容易产生误导——监控到"核心 topic A 出现消费延迟"，但根因可能完全
  在于"次要 topic B 的消息处理异常"，两者表面上毫无关联；
- 从架构设计角度看，**为同一个 consumer 订阅"重要性/稳定性差异很大的多个 topic"
  是一种隐性的可用性风险**，建议按业务重要性/故障隔离边界拆分为不同的
  `@KafkaListener`（即便共享同一个 `group.id` 也可以做到大幅降低影响）。

场景七实测证明了"拆分独立 `@KafkaListener`"这一缓解手段**确实有效**：拆分后
两个 topic 的 revoke 集合再也没有混在一起过，`other-events` 的消费吞吐几乎不受
影响（最大间隔 520ms，对比场景六的 10.4 秒，相差约 650 倍）。但也发现了一个
更精确的边界条件：**同一个 consumer group 的"控制面"（JoinGroup/SyncGroup
协议）仍然是共享的**，如果拆分出来的另一个 consumer 恰好被选为该 consumer
group 的 **leader**，它需要在组内任何成员触发 rebalance 时"陪同参与"一次
（负责为全组重新计算分配方案），代价是一次耗时在几十毫秒级、offset 精确衔接
不丢失的短暂 revoke——这与场景六里因为**自己超时被踢出**而发生的、耗时数秒的
revoke 在性质上完全不同，实践中通常可以接受。如果业务要求做到"控制面也完全
零感知"的极致隔离，需要把不同重要性的 topic 拆分到不同的 consumer group。

---

## 缓解建议汇总

| 风险成因 | 对应场景 | 建议缓解措施 |
|---|---|---|
| 单条消息处理耗时不可控（依赖第三方/大计算量） | 场景一 | 异步化处理 + 尽快 ack；或将耗时任务转移出监听线程（如投递到内部队列/线程池，主线程快速返回） |
| `max.poll.records` 设置过大 | 场景二 | 结合"最坏情况下单条处理耗时 × max.poll.records"反推 `max.poll.interval.ms`，或改用较保守的 `max.poll.records` |
| 阻塞式重试的单次等待时间设置不当 | 场景三 | 重试 backoff 的**单次**最大等待时间必须显著小于 `max.poll.interval.ms`；耗时较长的重试建议改为异步重试（如 Spring Kafka 的 Retry Topic / 非阻塞重试机制），避免占用主消费线程 |
| 下游服务临时不可用，需要较长时间等待恢复 | 场景四 | 使用 `ExponentialBackOffWithMaxRetries` 并将 `maxInterval` 控制在 `max.poll.interval.ms` 的 1.5～2 倍安全余量以内；可放心地让总重试时长远超过 `max.poll.interval.ms`（只要单次等待不超标） |
| 忽略了"处理耗时 + 退避等待"需要相加计算 | 场景五 | 设计 `max.poll.interval.ms` 安全余量时，必须用"最坏情况下处理耗时（含抛异常前的耗时）+ 单次退避等待时间"之和来对照阈值，而非只考虑退避时间本身 |
| 同一 consumer 订阅多个 topic，一个 topic 的问题拖累其它 topic | 场景六 | 按业务重要性/稳定性拆分为不同的 `@KafkaListener`（各自独立订阅、独立 consumer 实例，效果见场景七实测），或将 `partition.assignment.strategy` 显式设置为 `CooperativeStickyAssignor`（且不与仅支持 EAGER 的分配器混用）以启用增量再均衡，减少无关 topic 被牵连的概率 |
| 拆分独立 `@KafkaListener` 后，group leader 仍会经历短暂的"陪同参与" | 场景七 | 这种影响耗时通常在几十毫秒级、offset 精确衔接不丢失，一般可以接受；如需彻底隔离控制面，考虑把不同重要性的 topic 拆到不同的 consumer group（代价是无法共享同一套消费位点管理，需按业务权衡） |
| 消息重复处理 | 全部场景 | 消费逻辑必须幂等；关键业务配合去重表/状态机/唯一约束 |
| Rebalance 期间消费空窗（含跨 topic 的连带影响） | 场景一、二、三、六 | 监控 consumer lag 与 rebalance 频率指标（含"未直接触发异常但同属一个 consumer 订阅"的其它 topic）；评估是否可切换到 `CooperativeStickyAssignor` 等增量再均衡策略以减少"stop-the-world"影响 |

---

## 附：本项目涉及参数的 Spring Kafka / Kafka Client 官方默认值

以下默认值均通过直接读取本项目实际使用的依赖版本（Kafka client 4.1.2、
spring-kafka 4.0.4、spring-boot-kafka 4.0.4、spring-core 7.0.6）的源码 /
编译后的 `ConfigDef` 常量核实，而非二手资料，供对照本项目各场景 YAML 中
显式覆盖的数值时参考"如果不配置，默认是什么"。

### Kafka Consumer 相关（`org.apache.kafka.clients.consumer.ConsumerConfig`）

| 参数 | 官方默认值 | 本项目是否覆盖 | 说明 |
|---|---|---|---|
| `max.poll.interval.ms` | **300,000 ms（5 分钟）** | 是，各场景改为 6000~20000ms 以便在几十秒内复现现象 | 两次 `poll()` 之间允许的最大间隔，超过会触发 rebalance |
| `max.poll.records` | **500** | 是，各场景改为 1 或 10 | 一次 `poll()` 最多返回的记录数 |
| `session.timeout.ms` | **45,000 ms** | 是，改为 10000~12000ms | 心跳判定超时阈值；心跳线程独立运行，通常不受本项目场景影响 |
| `heartbeat.interval.ms` | **3,000 ms** | 否，本项目场景均保持默认值 3000ms | 心跳发送间隔，通常建议不超过 `session.timeout.ms` 的 1/3 |
| `enable.auto.commit` | **true** | 是，本项目全部场景显式设为 `false` | 本项目使用手动 ack（`manual_immediate`），必须关闭自动提交 |
| `auto.offset.reset` | **latest** | 是，本项目全部场景显式设为 `earliest` | 便于每次从头消费演示数据，不遗漏消息 |
| `fetch.max.wait.ms` | 500 ms | 否 | 一次 fetch 请求在数据不足时的最大等待时间，与 `max.poll.interval.ms` 无直接关系 |
| `request.timeout.ms` | 30,000 ms | 否 | 单次网络请求超时，与消费者组超时机制相互独立 |
| `retry.backoff.ms` | 100 ms | 否 | 客户端网络请求失败后的重试间隔（Kafka client 层面），与本文讨论的 `DefaultErrorHandler` 业务级重试是完全不同的两套机制，不要混淆 |
| `partition.assignment.strategy` | **`[RangeAssignor, CooperativeStickyAssignor]`** | 否，全部场景保持默认值 | **场景六的决定性配置项**：默认列表中 `RangeAssignor` 只支持 EAGER 协议，取交集后整体协议被锁定为 EAGER，导致"一个 topic 触发 rebalance 会牵连同一 consumer 订阅的所有 topic"；若只保留 `CooperativeStickyAssignor` 可切换为 COOPERATIVE 协议 |
| `group.protocol` | **`classic`** | 否，全部场景保持默认值 | Kafka 2.8+ 引入的新一代消费者组协议开关（KIP-848），另一个可选值是 `consumer`（架构上完全不同的服务端驱动增量分配协议）；本文档全部实验都基于默认的 `classic` 协议栈 |

### Spring Kafka 监听容器相关（`org.springframework.kafka.listener.ContainerProperties`）

| 参数 | 官方默认值 | 本项目是否覆盖 |说明 |
|---|---|---|---|
| `ackMode` | **`AckMode.BATCH`** | 是，全部场景改为 `MANUAL_IMMEDIATE` | 默认在每批 poll 处理完毕后自动提交；本项目需要精确控制每条消息提交的时机，因此改为手动 |
| `concurrency`（Spring Boot 属性 `spring.kafka.listener.concurrency`） | 未显式设置时为 **1** | 是，显式设为 1（与默认值相同，仅为清晰起见写出） | 每个 `@KafkaListener` 启动的消费者线程数 |

### Spring Kafka 错误处理相关（`org.springframework.kafka.listener.DefaultErrorHandler` / `SeekUtils`）

| 参数 | 官方默认值 | 本项目是否覆盖 |
|---|---|---|
| `DefaultErrorHandler` 无参构造时的默认 BackOff | **`FixedBackOff(0, 9)`**（即间隔 0ms、重试 9 次，共 10 次投递尝试） | 是，场景三、四、五都显式传入了自定义 `BackOff` 实例 |

### Spring `BackOff` 实现类默认值（`org.springframework.util.backoff`）

| 参数 | 所属类 | 官方默认值 |
|---|---|---|
| `initialInterval` | `ExponentialBackOff` | **2,000 ms** |
| `multiplier` | `ExponentialBackOff` | **1.5** |
| `maxInterval` | `ExponentialBackOff` | **30,000 ms** |
| `maxElapsedTime` | `ExponentialBackOff` | `Long.MAX_VALUE`（不限制总耗时） |
| `maxAttempts` | `ExponentialBackOff` | `Long.MAX_VALUE`（不限制重试次数） |
| `interval` | `FixedBackOff` | 需要显式传入，无内置默认值（构造函数强制要求） |

> 注：`ExponentialBackOffWithMaxRetries`（场景四使用）是 `ExponentialBackOff` 的子类，
> 通过构造函数传入 `maxRetries` 后会自动反推并锁定 `maxElapsedTime`，其余参数
> （`initialInterval`/`multiplier`/`maxInterval`）若不显式设置，仍沿用上表中
> `ExponentialBackOff` 的默认值。

---

## 附：七份原始日志与运行参数对照

| 文件 | Profile | 关键参数 | 观察到的 rebalance 次数 | 说明 |
|---|---|---|---|---|
| `docs/scenario1.log` | `scenario1` | `max.poll.records=1`，`max.poll.interval.ms=6000` | 18 次 | 单条 slow=true 消息处理 10s 直接超时；出现"毒消息"死循环现象 |
| `docs/scenario2.log` | `scenario2` | `max.poll.records=10`，`max.poll.interval.ms=6000` | 2 次 | 单条 800ms 不慢，批量累计 8000ms 超时；全部 15 条消息最终成功处理，但首次 rebalance 边界处有 3 条消息（offset 8~10）被重复投递处理，详见场景二补充分析 |
| `docs/scenario3.log` | `scenario3` | `max.poll.records=10`，`max.poll.interval.ms=8000`，`FixedBackOff(9000ms, 3次)` | 6 次 | 2 条"毒消息" × 最多 4 次尝试，每次单独等待即超时；其余 10 条消息正常提交 |
| `docs/scenario4.log` | `scenario4` | `max.poll.records=1`，`max.poll.interval.ms=6000`，`ExponentialBackOffWithMaxRetries(initial=500ms, multiplier=2, maxInterval=3000ms, maxRetries=8)` | **0 次** | 1 条 flaky 消息前 4 次尝试失败（模拟下游不可用），第 5 次尝试成功（模拟下游恢复）；全程 consumer generation 保持不变，3 条消息全部成功提交 offset |
| `docs/scenario5.log` | `scenario5` | `max.poll.records=1`，`max.poll.interval.ms=20000`，`FixedBackOff(10000ms, 3次)`，处理耗时固定 4000ms 后抛异常 | 0 次（阈值刻意放宽） | 精确测量退避等待与处理耗时的时间关系：两次"抛异常→下次开始处理"间隔均约 10000ms，证明退避时间**不会**扣除已消耗的处理时间；总间隔（处理+退避）约 14000ms |
| `docs/scenario6.log` | `scenario6` | `max.poll.records=1`，`max.poll.interval.ms=6000`；**一个** `@KafkaListener` 同时订阅 `slow-events` + `other-events` | 15 次 | `slow-events` 单条慢消息触发 rebalance（同场景一机制），`other-events` 完全健康但仍被一起 revoke/重新分配，实测消费中断约 10.4 秒；15 次 revoke 涉及的分区集合完全一致（两个 topic 的全部 6 个分区） |
| `docs/scenario7.log` | `scenario7` | 与场景六相同的配置和发送逻辑；但改为**两个独立** `@KafkaListener` 分别订阅两个 topic（共享 groupId） | `slow-events` 7 次，`other-events` 13 次（因 leader 陪同参与） | 两个 topic 的 revoke 集合从未混在一起；`other-events` 的 revoke→重新分配全程约 16ms，offset 精确衔接不丢失，消费吞吐最大间隔仅 520ms（对比场景六的 10.4 秒，相差约 650 倍） |

（日志文件较大，均已完整保留在 `docs/` 目录，关键片段的时间戳可用于交叉核对本报告中的分析。）
