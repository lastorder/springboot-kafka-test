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
├── KafkaRebalanceDemoApplication.kt   # 启动类
├── config/KafkaTopicConfig.kt         # 声明 3 分区的 slow-events topic
├── model/DemoEvent.kt                 # 事件数据模型
├── producer/DemoEventProducer.kt      # 启动后自动发送演示消息
└── listener/SlowEventListener.kt      # 监听消息，模拟"慢处理"
src/main/resources/
├── application.yml                    # Kafka 相关关键配置
└── logback-spring.xml                 # 日志配置，重点开启 rebalance 相关 logger
```

## 核心原理：如何真实触发 Rebalance

`application.yml` 中关键配置：

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

`DemoEventProducer` 启动后会向 `slow-events`（3 个分区）发送 12 条消息，其中约 1/4 标记为 `slow=true`。

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

### 3. 启动应用

```bash
./gradlew bootRun
```

应用启动后：
- `KafkaTopicConfig` 会自动创建 `slow-events`（3 分区）。
- `DemoEventProducer` 等待 3 秒后开始发送 12 条消息。
- `SlowEventListener` 开始消费，遇到 `slow=true` 的消息会阻塞 10 秒。

### 4. 观察日志中的 Rebalance 现象

正常情况下你会依次看到类似日志（真实日志会更详细，关键片段示意）：

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

`logback-spring.xml` 已将以下 logger 调至 `DEBUG`，可清晰观察整个过程：
- `org.apache.kafka.clients.consumer.internals.AbstractCoordinator`
- `org.apache.kafka.clients.consumer.internals.ConsumerCoordinator`
- `org.springframework.kafka.listener`
- `com.example.kafkarebalance`（业务日志）

### 5. 进阶：双实例观察"分区被抢占"式 Rebalance

在第一个实例仍在运行的情况下，用不同端口再启动一个实例（同一个 `group-id`）：

```bash
./gradlew bootRun --args='--server.port=8081'
```

此时两个 consumer 实例会共同瓜分 `slow-events` 的 3 个分区。当其中一个实例遇到慢消息、
处理超时被踢出组后，可以观察到：
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

测试仅验证 Spring 上下文能否正常加载（已排除 `DemoEventProducer` / `SlowEventListener` / `KafkaTopicConfig`
这几个依赖 Kafka 连接的 Bean），因此无需启动 Docker 中的 Kafka 即可运行通过。

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
