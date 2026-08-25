# springboot-kafka-test

演示 **Spring Boot 4** + **Kafka** 的消费监听项目，用于模拟"事件处理速度过慢导致 Consumer Group Rebalance"的场景，并配置好可观测 rebalance 全过程的日志。

## 技术栈

| 组件            | 版本                                  |
|-----------------|---------------------------------------|
| Spring Boot     | 4.0.4（Spring Framework 7）           |
| Kotlin          | 2.4.10                                |
| Java            | 25（Gradle toolchain，可自行调整）    |
| 构建工具        | Gradle 9.7.1（Kotlin DSL + Wrapper）  |
| Kafka Client    | 由 `spring-boot-starter-kafka` 管理   |
| 本地 Kafka      | Docker Compose（`apache/kafka`，KRaft 单节点）|

## 项目结构

```
src/main/kotlin/com/example/kafkarebalance/
├── KafkaRebalanceDemoApplication.kt        # 启动类
├── config/
│   ├── KafkaTopicConfig.kt                 # 声明 3 分区的 slow-events topic（三场景通用）
│   └── Scenario3ErrorHandlerConfig.kt      # 场景三：阻塞式重试 ErrorHandler（@Profile("scenario3")）
├── model/DemoEvent.kt                      # 事件数据模型（三场景通用）
├── producer/DemoEventProducer.kt           # 根据激活的 profile 采用不同发送策略
└── listener/
    ├── SlowEventListener.kt                # 场景一：单条处理过慢（@Profile("scenario1")）
    ├── BatchSlowEventListener.kt           # 场景二：批量累计耗时过长（@Profile("scenario2")）
    ├── RetryProneEventListener.kt          # 场景三：阻塞式重试导致超时（@Profile("scenario3")）
    └── TransientProcessingException.kt     # 场景三使用的可重试异常
src/main/resources/
├── application.yml                         # 公共配置（topic、序列化、默认 profile=scenario1）
├── application-scenario1.yml               # 场景一专属 Kafka 消费者配置
├── application-scenario2.yml               # 场景二专属 Kafka 消费者配置
├── application-scenario3.yml               # 场景三专属 Kafka 消费者配置
└── logback-spring.xml                      # 日志配置，重点开启 rebalance 相关 logger
```

## 三种 Rebalance 演示场景

本项目通过 Spring Profile（`scenario1` / `scenario2` / `scenario3`）切换三套独立的
消费者配置 + 监听器实现，分别演示三种导致 consumer group rebalance 的常见成因。
默认（不指定 profile）激活 `scenario1`。

| 场景 | max.poll.records | max.poll.interval.ms | 触发机制 | 启动命令 |
|------|-------------------|------------------------|----------|----------|
| 场景一 | 1  | 6000 | 单条消息处理耗时（10s）直接超过阈值 | `./gradlew bootRun --args='--spring.profiles.active=scenario1'` |
| 场景二 | 10 | 6000 | 单条不慢（800ms），但批量数量大，10 条累计 8000ms 超过阈值 | `./gradlew bootRun --args='--spring.profiles.active=scenario2'` |
| 场景三 | 10 | 8000 | 整体配置合理（10 条累计仅 2000ms），但阻塞式重试的等待耗时叠加导致超时 | `./gradlew bootRun --args='--spring.profiles.active=scenario3'` |

### 场景一：单条消息处理过慢（`SlowEventListener`）

`application-scenario1.yml` 关键配置：

```yaml
spring.kafka.consumer.properties:
  max.poll.interval.ms: 6000   # 两次 poll() 之间允许的最大间隔
  max.poll.records: 1          # 每次只拉 1 条，让"慢处理"影响更明显
  session.timeout.ms: 10000
  heartbeat.interval.ms: 3000
```

`SlowEventListener` 对标记为 `slow=true` 的消息会 `Thread.sleep(10_000)`（10 秒），
超过了 `max.poll.interval.ms=6000`（6 秒）。由于 Spring Kafka 的监听线程在处理消息期间不会调用 `poll()`，
一旦处理耗时超过该阈值，Kafka 的 group coordinator 会认为该 consumer 已经"卡死/失联"，
从而将其从消费组中踢出，触发一次真实的 **rebalance**（而不是人为断网/杀进程模拟）。

`DemoEventProducer` 在该场景下会向 `slow-events`（3 个分区）发送 12 条消息，其中约 1/4 标记为 `slow=true`。

### 场景二：单条不慢，但批量数量过大导致累计超时（`BatchSlowEventListener`）

这是很多人容易忽视的坑：**"每条消息处理都很快"不代表"批次整体不会超时"**。

`application-scenario2.yml` 关键配置：

```yaml
spring.kafka.consumer.properties:
  max.poll.interval.ms: 6000   # 阈值与场景一相同，便于对比
  max.poll.records: 10         # 一次 poll 最多拉取 10 条
```

`BatchSlowEventListener` 对每条消息固定处理 800ms（正常速度，单独看完全不慢）。
但由于 `max.poll.records=10`，一次 `poll()` 可能拉取 10 条消息，Spring Kafka 容器会在
**同一个消费线程、同一次 poll 循环内逐条同步派发**给监听方法处理——这是默认行为，无需开启批量监听模式：

```
10 条 × 800ms = 8000ms  >  max.poll.interval.ms(6000ms)  → 触发 rebalance
```

对比：如果把 `max.poll.records` 改回 1，单条 800ms 远小于 6000ms 阈值，则不会 rebalance。

`DemoEventProducer` 在该场景下会把 15 条消息全部发到**同一个分区**（`key="0"`）且几乎无发送间隔，
确保这些消息在 consumer 首次 `poll()` 之前就已全部写入 broker，从而一次 poll 能拉满 10 条，稳定复现该现象。

### 场景三：整体配置合理，但阻塞式重试导致超时（`RetryProneEventListener`）

这是最容易被忽视、也最贴近生产事故的场景：**表面上所有参数都配得"很保守很合理"，
但因为引入了同步阻塞式重试（`DefaultErrorHandler` + `FixedBackOff`），一条消息的失败重试
就能让整个批次的耗时暴涨，进而触发 rebalance**。

`application-scenario3.yml` 关键配置：

```yaml
spring.kafka.consumer.properties:
  max.poll.interval.ms: 8000   # 特意调大，体现"整体配置合理"
  max.poll.records: 10
```

正常情况下：10 条 × 200ms = 2000ms，远小于 8000ms 阈值，配置本身完全没有问题。

但 `Scenario3ErrorHandlerConfig` 注册了一个使用 `FixedBackOff(3000ms, maxAttempts=3)` 的
`DefaultErrorHandler`。当 `RetryProneEventListener` 遇到 `retryTrigger=true` 的消息时会抛出
`TransientProcessingException`，触发该 ErrorHandler 的重试逻辑：

```
1 次失败 + 最多 3 次重试 × (等待 3000ms + 处理 200ms) ≈ 9600ms
加上同批次其余消息的正常处理耗时（约 1800ms）
总计 ≈ 11400ms  >  max.poll.interval.ms(8000ms)  → 触发 rebalance
```

**关键原理**：`DefaultErrorHandler` 的重试是**同步阻塞**在当前消费线程里完成的——重试前的等待
并不会释放线程去调用 `KafkaConsumer#poll()`，因此重试耗时会直接累加到本次批次的总处理时间中。
这意味着：即便你把 `max.poll.records` 和 `max.poll.interval.ms` 都配得很"保守"，只要引入了
阻塞式重试 + 较长的 backoff，个别消息的失败仍然可能拖垮整个批次并触发 rebalance。

`DemoEventProducer` 在该场景下发送 12 条消息到同一分区，其中 index=0 和 index=6 两条消息
标记 `retryTrigger=true`，用于模拟批次中出现"瞬时失败"的情况。

## 快速开始

### 1. 前置条件

- JDK 25（或你本地已安装、适配 Gradle toolchain 的版本）
- Docker（用于启动本地 Kafka）

### 2. 启动本地 Kafka

```bash
docker compose up -d
```

首次启动等待几秒，可用以下命令确认健康：

```bash
docker compose ps
```

### 3. 启动应用（选择一个场景）

```bash
# 场景一：单条消息处理过慢
./gradlew bootRun --args='--spring.profiles.active=scenario1'

# 场景二：批量数量过大导致累计超时
./gradlew bootRun --args='--spring.profiles.active=scenario2'

# 场景三：整体配置合理，但阻塞式重试导致超时
./gradlew bootRun --args='--spring.profiles.active=scenario3'

# 不指定 profile 时默认等价于 scenario1
./gradlew bootRun
```

应用启动后（以 `scenario1` 为例）：
- `KafkaTopicConfig` 会自动创建 `slow-events`（3 分区）。
- `DemoEventProducer` 等待 3 秒后按当前场景发送对应的消息序列。
- 对应场景的监听器开始消费并演示相应的 rebalance 成因。

### 4. 观察日志中的 Rebalance 现象

#### 场景一日志示意

```
INFO  c.e.k.listener.SlowEventListener - 收到消息 id=xxx partition=0 slow=true
WARN  c.e.k.listener.SlowEventListener - 模拟慢处理开始：id=xxx 将阻塞 10000ms（超过 max.poll.interval.ms=6000ms，预期触发 rebalance）

... 约 6 秒后 ...

DEBUG o.a.k.c.consumer.internals.AbstractCoordinator - [Consumer clientId=..., groupId=slow-consumer-group] Sending Heartbeat request...
WARN  o.a.k.c.consumer.internals.ConsumerCoordinator - [Consumer ...] Member ... sending LeaveGroup request to coordinator ... due to consumer poll timeout has expired.
INFO  o.a.k.c.consumer.internals.ConsumerCoordinator - [Consumer ...] Revoke previously assigned partitions slow-events-0
INFO  o.s.k.listener.KafkaMessageListenerContainer - partitions revoked: [slow-events-0]

... 消费者重新加入组 ...

INFO  o.a.k.c.consumer.internals.ConsumerCoordinator - [Consumer ...] Successfully joined group with generation ...
INFO  o.a.k.c.consumer.internals.ConsumerCoordinator - [Consumer ...] Setting newly assigned partitions: slow-events-0, slow-events-1, slow-events-2
INFO  o.s.k.listener.KafkaMessageListenerContainer - partitions assigned: [slow-events-0, slow-events-1, slow-events-2]

WARN  c.e.k.listener.SlowEventListener - 提交 offset 失败，可能因 rebalance 导致该分区已被回收 id=xxx
```

#### 场景二日志示意

```
INFO  c.e.k.listener.BatchSlowEventListener - 收到消息 id=xxx partition=0 本批次第 1 条，处理耗时固定 800ms
WARN  c.e.k.listener.BatchSlowEventListener - 消息 id=xxx 处理完成，本批次累计耗时约 800ms（第 1 条）
INFO  c.e.k.listener.BatchSlowEventListener - 收到消息 id=xxx partition=0 本批次第 2 条，处理耗时固定 800ms
...
WARN  c.e.k.listener.BatchSlowEventListener - 消息 id=xxx 处理完成，本批次累计耗时约 8000ms（第 10 条）

... 累计耗时超过 max.poll.interval.ms=6000ms 之后 ...

WARN  o.a.k.c.consumer.internals.ConsumerCoordinator - [Consumer ...] Member ... sending LeaveGroup request to coordinator ... due to consumer poll timeout has expired.
INFO  o.s.k.listener.KafkaMessageListenerContainer - partitions revoked: [slow-events-0]
```

#### 场景三日志示意

```
INFO  c.e.k.listener.RetryProneEventListener - 收到消息 id=xxx partition=0 retryTrigger=true
ERROR c.e.k.listener.RetryProneEventListener - 模拟处理失败：id=xxx，将抛出可重试异常，触发 DefaultErrorHandler 的阻塞式重试（每次等待 3000ms，最多 3 次）
WARN  c.e.k.config.Scenario3ErrorHandlerConfig - 消息 offset=0 partition=0 第 1 次投递失败，将进行阻塞式重试等待 3000ms：模拟瞬时处理失败 id=xxx

... 等待 3000ms 后重试，再次失败 ...

WARN  c.e.k.config.Scenario3ErrorHandlerConfig - 消息 offset=0 partition=0 第 2 次投递失败，将进行阻塞式重试等待 3000ms：模拟瞬时处理失败 id=xxx
WARN  c.e.k.config.Scenario3ErrorHandlerConfig - 消息 offset=0 partition=0 第 3 次投递失败，将进行阻塞式重试等待 3000ms：模拟瞬时处理失败 id=xxx

... 累计耗时（约 9600ms + 其余消息处理时间）超过 max.poll.interval.ms=8000ms ...

WARN  o.a.k.c.consumer.internals.ConsumerCoordinator - [Consumer ...] Member ... sending LeaveGroup request to coordinator ... due to consumer poll timeout has expired.
INFO  o.s.k.listener.KafkaMessageListenerContainer - partitions revoked: [slow-events-0]
```

`logback-spring.xml` 已将以下 logger 调至 `DEBUG`，可清晰观察整个过程：
- `org.apache.kafka.clients.consumer.internals.AbstractCoordinator`
- `org.apache.kafka.clients.consumer.internals.ConsumerCoordinator`
- `org.springframework.kafka.listener`
- `com.example.kafkarebalance`（业务日志）

### 5. 进阶：双实例观察"分区被抢占"式 Rebalance

在第一个实例仍在运行的情况下，用不同端口、相同 profile 再启动一个实例（同一个 `group-id`）：

```bash
./gradlew bootRun --args='--spring.profiles.active=scenario1 --server.port=8081'
```

此时两个 consumer 实例会共同瓜分 `slow-events` 的 3 个分区。当其中一个实例触发上述任一
成因被踢出组后，可以观察到：
1. 该实例的分区被 revoke；
2. Coordinator 重新计算分配方案；
3. 存活的另一个实例接管这些分区（`partitions assigned` 中出现新的分区号）。

这更接近生产环境中"某个 Pod 处理慢，分区被自动转移到其他 Pod"的真实场景。

### 6. 清理环境

```bash
docker compose down -v
```

## 运行测试

```bash
./gradlew test
```

测试通过 `spring.profiles.active=test-none` 确保三个场景各自的 `@Profile` 监听器/ErrorHandler
均不会被装配，并排除 `DemoEventProducer` / `KafkaTopicConfig` 这两个依赖 Kafka 连接的公共 Bean，
因此无需启动 Docker 中的 Kafka 即可运行通过。


## 注意事项 / 已知说明

- `Thread.sleep` 直接阻塞消费线程仅用于**教学演示**"处理慢"的效果；生产代码中不应在
  `@KafkaListener` 方法内做同步阻塞式的耗时操作，应使用异步处理、限流、或调大
  `max.poll.interval.ms` 并配合合理的超时/重试策略。
- 手动 ack（`ack-mode: manual_immediate`）在 rebalance 发生后提交可能会抛出
  `CommitFailedException`，代码中已 try/catch 并以 `WARN` 记录，这是预期现象，不会导致
  监听容器整体崩溃。
- 如果本地未启动 `docker compose up -d`，应用日志会持续出现连接 `localhost:9092` 失败/重试的信息。
- Kotlin 2.4.10 对 JVM 25 字节码目标的支持情况可能随版本更新变化；如遇到
  `Kotlin does not yet support 25 JDK target` 之类的警告，属 Kotlin 编译器已知限制，
  通常会自动回退到其支持的最高版本，不影响功能正确性。
- **场景二、三强依赖"多条消息在同一次 poll 中被一起拉取"**。如果消息发送速度不够快，
  或消费者启动过早导致消息被拆成多个小批次，可能无法稳定复现累计超时现象。若未复现，
  可尝试：适当增大 `DemoEventProducer` 中的消息数量、缩短发送间隔，或延长消费者启动等待时间，
  确保消息在 consumer 首次 `poll()` 之前已全部写入 broker。
- **场景三的 `DefaultErrorHandler` 重试是同步阻塞式的**（基于 `FixedBackOff`/`ExponentialBackOffWithMaxRetries`），
  区别于 Spring Kafka 的"重试主题"（Retry Topic，异步、通过重新投递到独立 topic 实现），
  后者不会阻塞当前消费线程，也就不会导致本场景描述的这类 rebalance。如果你的生产环境使用的是
  重试主题机制，需要用不同的思路排查 rebalance 原因。

