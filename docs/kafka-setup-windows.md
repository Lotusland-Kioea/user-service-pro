# Apache Kafka 4.0.2 Windows 环境搭建指南

> 适用场景：Windows 11 单机部署，三节点 KRaft 集群（开发/学习环境）

## 前置条件

- **JDK**：已安装 JDK 21（路径 `D:\Develop\Java\jdk-21`），`JAVA_HOME` 已配置
- **磁盘**：至少 2 GB 空闲空间
- **解压工具**：Windows 11 内置 tar 或 7-Zip
- **本指南假定**：你已有 `D:\Develop` 目录用于存放开发软件

## 1. 下载 Kafka

访问 [Apache Kafka 下载页面](https://kafka.apache.org/downloads)，下载最新稳定版：

- **推荐版本**：`kafka_2.13-4.0.2.tgz`
- Scala 2.13 是 Kafka 4.0.x 的编译版本
- 不要下载源码包（`src` 后缀），下载二进制包

下载后保存到 `D:\Develop\` 目录。

## 2. 解压

打开 **PowerShell**（建议 Windows Terminal），执行：

```powershell
cd D:\Develop
tar -xzf kafka_2.13-4.0.2.tgz
```

解压后目录结构：

```
D:\Develop\kafka_2.13-4.0.2\
├── bin\windows\          ← 所有 .bat 命令在这里
├── config\kraft\         ← KRaft 模式配置模板
├── libs\                 ← Kafka 依赖 JAR
└── licenses\
```

### 设置 PATH（可选，建议）

```powershell
# 以管理员身份运行 PowerShell，执行：
[Environment]::SetEnvironmentVariable("Path", $env:Path + ";D:\Develop\kafka_2.13-4.0.2\bin\windows", "User")
```

设置后重新打开终端，即可在任意目录直接使用 `kafka-topics.bat` 等命令。

> 如果没有设置 PATH，后续所有命令需要切换到 `D:\Develop\kafka_2.13-4.0.2\bin\windows` 目录执行，或使用完整路径。

## 3. 理解 KRaft 模式

Kafka 4.0 **已彻底移除 ZooKeeper**，KRaft 是唯一模式。

**KRaft vs 旧版 ZooKeeper：**

| | ZooKeeper 模式（Kafka 3.x 以前） | KRaft 模式（Kafka 3.3+ / 4.0 强制） |
|---|---|---|
| 元数据存储 | 外部 ZooKeeper 集群 | Kafka 内置 Raft 共识 |
| 进程数 | Kafka + ZooKeeper | 仅 Kafka |
| 配置复杂度 | 高（两套系统） | 低（一套系统） |
| Controller 选举 | 依赖 ZooKeeper | 内建 Raft 选举 |

每个节点可以承担两种角色：
- **Broker**：处理消息读写
- **Controller**：管理集群元数据（Leader 选举、分区分配等）

本指南采用 **组合模式**（`broker,controller`），每个节点同时承担两种角色。

## 4. 配置三节点集群

### 4.1 集群拓扑

```
Windows 物理机 (localhost)
├── 节点 1: PLAINTEXT 9092 / CONTROLLER 9093
├── 节点 2: PLAINTEXT 9094 / CONTROLLER 9095
└── 节点 3: PLAINTEXT 9096 / CONTROLLER 9097

所有节点 process.roles=broker,controller (组合模式)
```

### 4.2 创建数据目录

```powershell
mkdir D:\Develop\kafka_2.13-4.0.2\data\kraft-1
mkdir D:\Develop\kafka_2.13-4.0.2\data\kraft-2
mkdir D:\Develop\kafka_2.13-4.0.2\data\kraft-3
```

### 4.3 配置三个节点

将以下配置文件保存到 `D:\Develop\kafka_2.13-4.0.2\config\kraft\` 目录。

> 本目录 `docs/` 下提供了三个配置模板文件：`server-1.properties`、`server-2.properties`、`server-3.properties`，可以直接复制使用。

#### 节点 1（复制为 `config/kraft/server-1.properties`）

```properties
# ======== KRaft 模式 ========
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093,2@localhost:9095,3@localhost:9097

# ======== 网络监听 ========
listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093
advertised.listeners=PLAINTEXT://localhost:9092
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT

# ======== 存储路径 ========
log.dirs=D:/Develop/kafka_2.13-4.0.2/data/kraft-1
metadata.log.dir=D:/Develop/kafka_2.13-4.0.2/data/kraft-1

# ======== 线程配置 ========
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

# ======== 集群级 Topic 默认配置 ========
num.partitions=3
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
```

#### 节点 2（复制为 `config/kraft/server-2.properties`）

```properties
# ======== KRaft 模式 ========
process.roles=broker,controller
node.id=2
controller.quorum.voters=1@localhost:9093,2@localhost:9095,3@localhost:9097

# ======== 网络监听 ========
listeners=PLAINTEXT://localhost:9094,CONTROLLER://localhost:9095
advertised.listeners=PLAINTEXT://localhost:9094
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT

# ======== 存储路径 ========
log.dirs=D:/Develop/kafka_2.13-4.0.2/data/kraft-2
metadata.log.dir=D:/Develop/kafka_2.13-4.0.2/data/kraft-2

# ======== 线程配置 ========
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

# ======== 集群级 Topic 默认配置 ========
num.partitions=3
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
```

#### 节点 3（复制为 `config/kraft/server-3.properties`）

```properties
# ======== KRaft 模式 ========
process.roles=broker,controller
node.id=3
controller.quorum.voters=1@localhost:9093,2@localhost:9095,3@localhost:9097

# ======== 网络监听 ========
listeners=PLAINTEXT://localhost:9096,CONTROLLER://localhost:9097
advertised.listeners=PLAINTEXT://localhost:9096
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT

# ======== 存储路径 ========
log.dirs=D:/Develop/kafka_2.13-4.0.2/data/kraft-3
metadata.log.dir=D:/Develop/kafka_2.13-4.0.2/data/kraft-3

# ======== 线程配置 ========
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

# ======== 集群级 Topic 默认配置 ========
num.partitions=3
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
```

## 5. 格式化并启动集群

### 5.1 生成集群 UUID

```powershell
cd D:\Develop\kafka_2.13-4.0.2\bin\windows
.\kafka-storage.bat random-uuid
```

输出示例：`keuRGfK5RPmjohSucR1Cfg`

**⚠️ 保存这个 UUID！三个节点必须使用相同的 UUID。**

以下命令中，将 `<你的UUID>` 替换为上一步生成的 UUID。

### 5.2 格式化三个节点的存储目录

```powershell
.\kafka-storage.bat format -t <你的UUID> -c ..\..\config\kraft\server-1.properties
.\kafka-storage.bat format -t <你的UUID> -c ..\..\config\kraft\server-2.properties
.\kafka-storage.bat format -t <你的UUID> -c ..\..\config\kraft\server-3.properties
```

每条命令输出应显示 `Formatting the storage directory...` 且无报错。

### 5.3 启动三个节点

打开 **三个独立的终端窗口**，在每个终端中分别执行：

**终端 1：**
```powershell
cd D:\Develop\kafka_2.13-4.0.2\bin\windows
.\kafka-server-start.bat ..\..\config\kraft\server-1.properties
```

**终端 2：**
```powershell
cd D:\Develop\kafka_2.13-4.0.2\bin\windows
.\kafka-server-start.bat ..\..\config\kraft\server-2.properties
```

**终端 3：**
```powershell
cd D:\Develop\kafka_2.13-4.0.2\bin\windows
.\kafka-server-start.bat ..\..\config\kraft\server-3.properties
```

每个终端会输出大量启动日志。看到 `Kafka Server started` 即表示该节点启动成功。

## 6. 验证集群

打开 **第四个终端**，执行以下验证命令：

### 6.1 检查集群元数据状态

```powershell
cd D:\Develop\kafka_2.13-4.0.2\bin\windows
.\kafka-metadata-quorum.bat --bootstrap-server localhost:9092 describe --status
```

期望输出：显示 3 个节点，`Status: Cluster is healthy`

### 6.2 检查 Broker 可用性

```powershell
.\kafka-broker-api-versions.bat --bootstrap-server localhost:9092
```

应显示三个 broker（分别在 9092、9094、9096 端口）的 API 版本信息。

## 7. 创建测试 Topic 并验证副本机制

### 7.1 创建 Topic

```powershell
.\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic test --partitions 3 --replication-factor 3
```

### 7.2 查看 Topic 详情（观察 Leader 和 ISR）

```powershell
.\kafka-topics.bat --bootstrap-server localhost:9092 --describe --topic test
```

输出示例：

```
Topic: test  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
Topic: test  Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
Topic: test  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
```

字段解释：
- **Leader**：负责该分区所有读写的节点
- **Replicas**：拥有该分区副本的所有节点
- **Isr**（In-Sync Replicas）：与 Leader 完全同步的副本集合

### 7.3 生产/消费测试

**生产消息：**
```powershell
.\kafka-console-producer.bat --bootstrap-server localhost:9092 --topic test
```
然后输入几行文本，按 `Ctrl+C` 退出。

**消费消息（另一个终端）：**
```powershell
.\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test --from-beginning
```
应看到刚输入的消息。

## 8. 演示 Leader 选举（关键学习环节）

### 步骤 1：再次查看当前 Leader 分布

```powershell
.\kafka-topics.bat --bootstrap-server localhost:9092 --describe --topic test
```

记住各分区的 Leader 分布。

### 步骤 2：模拟节点故障

关闭节点 1 的终端窗口（或按 `Ctrl+C`）。

### 步骤 3：观察 Leader 变更

```powershell
.\kafka-topics.bat --bootstrap-server localhost:9094 --describe --topic test
```

> 注意：此时需连接存活节点（9094 或 9096）

应看到原本在节点 1 上的 Leader 已漂移到节点 2 或 3，ISR 中节点 1 不再出现。

### 步骤 4：测试高可用——故障期间仍可读写

```powershell
# 生产
.\kafka-console-producer.bat --bootstrap-server localhost:9094,localhost:9096 --topic test --producer-property acks=all

# 消费
.\kafka-console-consumer.bat --bootstrap-server localhost:9094,localhost:9096 --topic test --from-beginning
```

### 步骤 5：恢复节点

重启节点 1（重新执行 `kafka-server-start.bat ..\..\config\kraft\server-1.properties`）。

### 步骤 6：观察 ISR 恢复

```powershell
.\kafka-topics.bat --bootstrap-server localhost:9092 --describe --topic test
```

节点 1 重新出现在 ISR 列表中，作为 Follower 保持同步。

## 9. 创建应用所需的 Topic

```powershell
cd D:\Develop\kafka_2.13-4.0.2\bin\windows

# 用户事件 Topic（业务用）
.\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic user-events --partitions 3 --replication-factor 3

# 用户事件死信队列
.\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic user-events-dlt --partitions 1 --replication-factor 3

# 实验消息 Topic（实验室用）
.\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic lab-messages --partitions 4 --replication-factor 3

# 实验死信队列
.\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic lab-messages-dlt --partitions 1 --replication-factor 3
```

验证：

```powershell
.\kafka-topics.bat --bootstrap-server localhost:9092 --list
```

应显示 4 个 Topic。

## 10. 常见问题排查

### 端口被占用

```powershell
netstat -ano | findstr :9092
```

找到占用端口的 PID 后终止：

```powershell
taskkill /F /PID <PID>
```

### 启动失败：路径错误

确保配置文件中所有路径使用正斜杠 `/`（不要用反斜杠 `\`）。

### 存储目录权限问题

删除数据目录下的所有文件后重新格式化：

```powershell
rmdir /s /q D:\Develop\kafka_2.13-4.0.2\data\kraft-1
rmdir /s /q D:\Develop\kafka_2.13-4.0.2\data\kraft-2
rmdir /s /q D:\Develop\kafka_2.13-4.0.2\data\kraft-3
```

然后重新创建目录并执行 `kafka-storage.bat format`。

### 节点无法加入集群

- 检查三个节点的 `controller.quorum.voters` 配置是否完全一致
- 检查是否使用相同的集群 UUID 格式化
- 检查防火墙是否阻止了 controller 端口（9093/9095/9097）

### Kafka 进程后台残留

```powershell
# 查看所有 Java 进程
tasklist | findstr java

# 终止所有 Java 进程（谨慎使用）
taskkill /F /IM java.exe
```

## 11. 关闭集群

每个终端按 `Ctrl+C` 优雅关闭。Kafka 会处理正在写入的数据并安全退出。

关闭顺序：先关闭 Controller 节点（节点 3、2），最后关闭节点 1。

## 下一步

环境就绪后，返回 [README.md](../README.md) 查看 v1.1.0 的应用配置说明。
