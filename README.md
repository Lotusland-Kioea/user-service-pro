# user-service-pro

Spring Boot 3.4.5 + MyBatis-Plus + Redis + Vue 3 用户管理服务，采用 Git Tag 版本管理，持续迭代。

> 当前最新版本：[v1.1.0](https://github.com/Lotusland-Kioea/user-service-pro/releases/tag/v1.1.0)  
> 查看历史版本：`git checkout v0.1.0` / `v1.0.0`

---

## 项目定位

本项目是 **AI 辅助编程（AICoding）学习实践**，通过 Claude Code 的 Plan / Subagent / Code Review 等技能，从零搭建一个可演进的 Spring Boot 工程脚手架。每个版本通过 Git Tag 归档，可独立检出运行。

### 学习目标

**工程基础（v1.0.0）**
- 搭建规范的 Spring Boot 后端工程骨架（分层、校验、异常、日志、测试）
- 跑通单机 MySQL + Redis 缓存，理解 MyBatis-Plus 的 ORM 能力
- 搭建 Vue 3 + Element Plus 前端，实现前后端联调
- 熟悉 Claude Code 开发流程：Plan（方案设计）→ Subagent（并行开发）→ Code Review（审查修复）
- 实践 Git 版本管理：Tag 归档、多版本共存、云端备份

**消息队列（v1.1.0）**
- 从零搭建 Apache Kafka 4.0 集群（Windows 原生、KRaft 模式、单机三节点）
- 理解并实践事件驱动架构：领域事件 → Spring ApplicationEvent → Kafka 异步消息
- 掌握生产级 Kafka 配置：acks、幂等生产者、事务、手动 ACK、DLT 死信队列
- 理解 Kafka 高可用原理：副本、ISR、Leader 选举、重平衡，并通过故障演练验证
- 通过双轨架构（业务集成 + 独立实验室）系统学习 Kafka 各概念
- 体验 Code Review 驱动的 Bug 修复流程：两轮审查发现 6 个缺陷并全部修复

---

## 技术栈

| 层级 | 技术 | 版本 |
|---|---|---|
| 后端框架 | Spring Boot | 3.4.5 |
| ORM | MyBatis-Plus | 3.5.7 |
| 数据库 | MySQL | 8.x |
| 缓存 | Redis | Windows build |
| 消息队列 | Apache Kafka | 4.0.2 (KRaft) |
| 前端 | Vue 3 + Vite | 3.x |
| UI 库 | Element Plus | latest |
| HTTP 客户端 | axios | latest |
| 测试 | JUnit 5 + Mockito + MockMvc + EmbeddedKafka | — |

---

## 本地环境

| 工具 | 路径 | 版本 |
|---|---|---|
| Java | `D:\Develop\Java\jdk-26.0.1+8` | OpenJDK 26 |
| Maven | `D:\Develop\Maven` | — |
| Node.js | `D:\Develop\Nodejs` | v24.16.0 |
| MySQL | `D:\Develop\MySQL` | 8.x |
| Redis | `D:\Develop\Redis` | 5.0.14 (Windows) |
| Kafka | `D:\Develop\kafka\kafka_2.13-4.0.2` | 4.0.2 (KRaft) |
| IntelliJ IDEA | `D:\Develop\JetBrains\IntelliJ IDEA` | — |
| VS Code | `D:\Develop\Microsoft VS Code` | — |

---

## 数据库初始化

```sql
CREATE DATABASE IF NOT EXISTS mydb DEFAULT CHARACTER SET utf8mb4;
USE mydb;

CREATE TABLE IF NOT EXISTS users (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(200) NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted INT DEFAULT 0 COMMENT '逻辑删除: 0=正常, 1=已删除',
    INDEX idx_deleted (deleted)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

---

## 项目结构

```
user-service-pro/
├── pom.xml
├── .gitignore
├── README.md
├── src/main/java/com/example/userservice/
│   ├── DemoApplication.java              ← 启动入口
│   ├── common/
│   │   ├── ApiResponse.java              ← 统一响应体
│   │   ├── CacheConstants.java           ← 缓存 key 常量
│   │   └── KafkaTopicConstants.java      ← Kafka Topic 常量 [v1.1.0]
│   ├── config/
│   │   ├── MyBatisPlusConfig.java        ← 分页插件 + 自动填充
│   │   ├── RedisConfig.java              ← Redis 缓存配置
│   │   ├── WebConfig.java                ← CORS 跨域
│   │   ├── KafkaConfig.java              ← Kafka 核心配置 [v1.1.0]
│   │   └── KafkaTopicConfig.java         ← NewTopic Bean [v1.1.0]
│   ├── controller/
│   │   └── UserController.java           ← REST 控制器
│   ├── dto/
│   │   ├── CreateUserRequest.java        ← 创建请求 DTO
│   │   └── UpdateUserRequest.java        ← 更新请求 DTO
│   ├── entity/
│   │   └── User.java                     ← 数据库实体
│   ├── event/                            ← [v1.1.0] 领域事件
│   │   ├── UserEvent.java                ← 事件 POJO
│   │   └── UserEventType.java            ← 事件类型枚举
│   ├── exception/
│   │   ├── BusinessException.java        ← 业务异常
│   │   └── GlobalExceptionHandler.java   ← 全局异常处理
│   ├── lab/                              ← [v1.1.0] Kafka 实验室
│   │   ├── KafkaLabController.java       ← REST 控制器 /kafka-lab
│   │   ├── KafkaLabProducer.java         ← 实验室生产者
│   │   ├── KafkaLabConsumer.java         ← 实验室消费者
│   │   └── LabMessage.java               ← 消息 DTO
│   ├── mapper/
│   │   └── UserMapper.java               ← MyBatis-Plus Mapper
│   ├── messaging/                        ← [v1.1.0] 消息层
│   │   ├── UserEventPublisher.java       ← Kafka 生产者
│   │   ├── UserEventConsumer.java        ← Kafka 消费者
│   │   ├── UserEventKafkaBridge.java     ← 事务事件 → Kafka 桥接
│   │   └── KafkaErrorHandler.java        ← DLT + 重试处理器
│   ├── service/
│   │   ├── UserService.java              ← 服务接口
│   │   └── impl/UserServiceImpl.java     ← 服务实现（含事件发布 [v1.1.0]）
│   └── vo/
│       └── UserVO.java                   ← 视图对象
├── src/main/resources/
│   ├── application.yml                   ← 公共配置
│   ├── application-dev.yml               ← 开发环境配置
│   └── application-kafka.yml             ← Kafka 配置 [v1.1.0]
├── src/test/java/com/example/userservice/
│   ├── controller/
│   │   └── UserControllerTest.java       ← Controller 测试（8 个）
│   ├── service/
│   │   └── UserServiceTest.java          ← Service 测试（7 个）
│   ├── messaging/                        ← [v1.1.0]
│   │   ├── UserEventConsumerTest.java    ← 消费者测试（4 个）
│   │   ├── UserEventPublisherTest.java   ← 生产者测试（3 个）
│   │   └── UserEventKafkaBridgeTest.java ← 桥接测试（2 个）
│   └── lab/                              ← [v1.1.0]
│       └── KafkaLabControllerTest.java   ← 实验室测试（3 个）
└── frontend/                             ← Vue 3 前端
    ├── vite.config.js                     ← Vite + API 代理配置
    ├── index.html
    └── src/
        ├── main.js                        ← Element Plus 注册
        ├── App.vue                        ← 用户管理主页面
        └── api/
            ├── index.js                   ← axios 封装 + 响应拦截
            └── userApi.js                 ← 用户 API 方法
```

---

## 启动方式

### 后端

**推荐：IntelliJ IDEA**

1. 打开 `E:\Project\user-service-pro`
2. 设置环境变量：Run → Edit Configurations → Environment variables → 添加 `MYSQL_PASSWORD=你的MySQL密码`
3. 右键 `DemoApplication.java` → Run
4. 控制台输出：
```
========================================
  user-service-pro v1.1.0 启动成功!
  API:      http://localhost:8080/users
  数据库:   localhost:3306/mydb
  Redis:    localhost:6379
  Kafka:    localhost:9092,9094,9096
  KafkaLab: http://localhost:8080/kafka-lab
========================================
```

**命令行：**
```bash
cd E:\Project\user-service-pro
mvn spring-boot:run
```

**运行测试：**
```bash
mvn clean test
# 预期：Tests run: 27, Failures: 0, Errors: 0, Skipped: 0
```

### 前端

```bash
cd frontend
npm install       # 仅首次
npm run dev       # → http://localhost:5173
```

### 联调

1. IDEA 启动后端（端口 8080，记得设置 `MYSQL_PASSWORD` 环境变量）
2. 终端 `cd frontend && npm run dev`（端口 5173）
3. 浏览器访问 `http://localhost:5173`
4. Vite 自动将 `/api` 请求代理到后端

---

## API 接口

所有接口返回统一格式：

```json
{
  "code": 200,
  "message": "操作成功",
  "data": { ... }
}
```

| 方法 | 路径 | 参数 | 说明 |
|---|---|---|---|
| `POST` | `/users` | Body: `{"name":"","email":""}` | 创建用户 |
| `GET` | `/users` | Query: `?page=1&size=10&keyword=xxx` | 分页搜索 |
| `GET` | `/users/{id}` | Path: id | 单用户查询 |
| `PUT` | `/users/{id}` | Body: `{"name":"","email":""}` | 更新用户 |
| `DELETE` | `/users/{id}` | Path: id | 逻辑删除 |

### 错误码

| code | HTTP 状态 | 说明 |
|---|---|---|
| 200 | 200 | 操作成功 |
| 400 | 400 | 参数校验失败 / 请求体格式错误 |
| 404 | 404 | 用户不存在 |
| 500 | 500 | 系统内部错误 |

---

## 版本记录

### v1.1.0（当前版本）

`git checkout v1.1.0` | [Release](https://github.com/Lotusland-Kioea/user-service-pro/releases/tag/v1.1.0)

引入 Apache Kafka 消息队列，实现事件驱动雏形，建立独立的消息实验室模块。

#### 一、新增能力

| 维度 | 内容 |
|---|---|
| **消息队列** | Apache Kafka 4.0.2，单机三节点 KRaft 集群（端口 9092/9094/9096），`replication-factor=3`, `min.insync.replicas=2` |
| **集群管理** | `D:\Develop\kafka\start-kafka-cluster.bat` 一键启动三节点，无需 ZooKeeper |
| **事件驱动** | User CRUD → `ApplicationEventPublisher` → `@TransactionalEventListener(AFTER_COMMIT)` → Kafka |
| **消费者** | `@KafkaListener` 异步消费，手动 ACK + 幂等去重 + DLT 死信队列 + 固定 2s × 3 次退避重试 |
| **生产级配置** | `acks=all`、`enable.idempotence=true`、`compression.type=snappy`、`isolation.level=read_committed`、`JavaTimeModule` 序列化、`AckMode.MANUAL`、`transaction.id.prefix` |
| **Kafka 实验室** | 8 个 REST 端点（`/kafka-lab/*`），独立演示同步/异步/事务/批量/分区路由/DLT |
| **新增包** | `event/`（领域事件）、`messaging/`（消息层）、`lab/`（学习实验室） |
| **新增文件** | 16 个 Java 源文件 + 4 个测试类 + 3 个配置文件 + 2 个启动脚本 + 1 个环境搭建文档 |
| **测试** | 27 个（v1.0.0 的 15 个 + 12 个新增），BUILD SUCCESS |

#### 二、新增/变更文件一览

```
新增文件：
├── docs/
│   ├── kafka-setup-windows.md          ← Kafka 集群搭建完整指南
│   ├── server-1.properties             ← 节点 1 配置模板
│   ├── server-2.properties             ← 节点 2 配置模板
│   └── server-3.properties             ← 节点 3 配置模板
├── src/main/java/.../common/
│   └── KafkaTopicConstants.java        ← Topic 名称常量
├── src/main/java/.../config/
│   ├── KafkaConfig.java                ← ★ 核心配置（生产者/消费者/容器工厂）
│   └── KafkaTopicConfig.java          ← NewTopic Bean 声明
├── src/main/java/.../event/
│   ├── UserEvent.java                  ← 领域事件 POJO
│   └── UserEventType.java             ← 事件类型枚举
├── src/main/java/.../messaging/
│   ├── UserEventPublisher.java         ← Kafka 生产者
│   ├── UserEventConsumer.java          ← Kafka 消费者（@KafkaListener）
│   ├── UserEventKafkaBridge.java       ← ★ Spring 事件 → Kafka 桥接
│   └── KafkaErrorHandler.java          ← DLT + 重试处理器
├── src/main/java/.../lab/
│   ├── KafkaLabController.java         ← /kafka-lab REST 控制器
│   ├── KafkaLabProducer.java           ← 实验室生产者
│   ├── KafkaLabConsumer.java           ← 实验室消费者（含批量消费）
│   └── LabMessage.java                 ← 实验室消息 DTO
├── src/main/resources/
│   └── application-kafka.yml           ← Kafka 生产级配置
├── src/test/.../messaging/
│   ├── UserEventConsumerTest.java
│   ├── UserEventPublisherTest.java
│   └── UserEventKafkaBridgeTest.java
├── src/test/.../lab/
│   └── KafkaLabControllerTest.java
└── src/test/resources/
    └── application-test.yml

修改文件：
├── pom.xml                             ← +spring-kafka, +spring-kafka-test, +surefire -XX:+EnableDynamicAgentLoading
├── .gitignore                          ← +start-app.bat
├── DemoApplication.java                ← 启动 banner 更新
├── UserServiceImpl.java                ← +ApplicationEventPublisher 事件发布
├── UserServiceTest.java                ← +@Mock ApplicationEventPublisher
├── application.yml                     ← +profiles.include: kafka
├── application-dev.yml                 ← +Kafka 注释
└── README.md                           ← 版本更新
```

#### 三、架构决策与经验

**1. 环境搭建：Windows 原生 vs Docker**

选择 Windows 原生安装（`D:\Develop\kafka\`），而非 Docker Compose：
- 让学习者直面 Kafka 的目录结构、配置文件、命令行工具，理解更深入
- JDK 26 + Mockito 兼容性问题（inline mock maker 失效）→ `mock-maker-subclass` + `-XX:+EnableDynamicAgentLoading`
- Kafka 4.0 已移除 ZooKeeper，KRaft 模式下仅需配置 `process.roles`、`controller.quorum.voters`

**2. 双轨架构：业务集成 + 独立实验室**

```
轨道 A（业务轨）：User CRUD → Spring Event → Bridge → Kafka → Consumer
轨道 B（实验室）：/kafka-lab/* → LabProducer → Kafka → LabConsumer
```

- 业务轨演示"真实项目中 Kafka 怎么用"（`@TransactionalEventListener(AFTER_COMMIT)` 保证 DB 事务提交后才发消息）
- 实验室轨让每个 Kafka 概念都能独立验证（同步/异步/事务/批量/分区/DLT）

**3. Code Review 发现的关键 Bug（已全部修复）**

| # | 严重度 | 问题 | 根因 | 修复 |
|---|---|---|---|---|
| 1 | 🔴 严重 | 消费者 "invokeHandler Failed" | `KafkaConfig` 未设置 `AckMode.MANUAL`，YAML 配置被 Java Bean 覆盖 | 在 `kafkaListenerContainerFactory` 和 `batchListenerContainerFactory` 中显式设置 `ContainerProperties.AckMode.MANUAL` |
| 2 | 🔴 严重 | DLT 投递到不存在的分区 | `DestinationResolver` 按原始分区号投递，但 DLT topic 只有 1 个分区（分区 0） | 改为 `new TopicPartition(topic, -1)` 让 Kafka 自动选择分区 |
| 3 | 🔴 严重 | `LocalDateTime` 序列化失败 | 自建 `DefaultKafkaProducerFactory`/`ConsumerFactory` 不自动注册 `JavaTimeModule` | 显式创建带 `JavaTimeModule` 的 `ObjectMapper` 传给 `JsonSerializer`/`JsonDeserializer` |
| 4 | 🟡 高 | 测试方法签名不匹配 | `UserEventConsumer.onUserEvent()` 改了参数，测试未同步 | 测试调用改为 `(event, ack)` 两参数版本 |
| 5 | 🟡 中 | Kafka 事务不生效 | 生产者缺少 `transaction.id.prefix` | 添加 `user-service-pro-tx-` 前缀 |
| 6 | 🟡 中 | `profiles.include: kafka` 影响测试环境 | YAML 无条件引入 `application-kafka.yml` | `application-test.yml` 排除 `KafkaAutoConfiguration` |

**4. IDEA 开发环境配置**

| 配置项 | 方式 | 说明 |
|---|---|---|
| 数据库密码 | 虚拟机选项 `-Dspring.datasource.password=<你的密码>` | 更可靠，比环境变量优先级高 |
| 环境变量 | 编辑配置 → 环境变量对话框 → `MYSQL_PASSWORD=<你的密码>` | 需要点击 📝 按钮逐行添加，直接写在输入框可能无效 |

**5. 启动顺序**

```
1. 双击 D:\Develop\kafka\start-kafka-cluster.bat  → 三节点 Kafka
2. IDEA 启动 DemoApplication                       → 后端 :8080
3. cd frontend && npm run dev                      → 前端 :5173
```

**环境搭建：** 参见 [docs/kafka-setup-windows.md](docs/kafka-setup-windows.md)

---

### v1.0.0

`git checkout v1.0.0` | [Release](https://github.com/Lotusland-Kioea/user-service-pro/releases/tag/v1.0.0)

首个工程化版本。在 v0.1.0 基础上完成规范化重构和前端搭建。

| 维度 | 内容 |
|---|---|
| 文件 | 16 个 Java 源文件 + 2 个测试类 + 13 个前端文件 |
| ORM | MyBatis-Plus 3.5.7（BaseMapper + 分页 + 逻辑删除 + 自动填充） |
| API | 5 个（POST/GET/GET{id}/PUT/DELETE）+ 分页搜索 |
| 分层 | DTO（Create/Update）→ Entity → VO 三层解耦 |
| 校验 | `@Valid` + Jakarta Bean Validation |
| 响应 | 统一 `ApiResponse<T>`（code + message + data） |
| 异常 | `@RestControllerAdvice` + `ResponseEntity` 动态 HTTP 状态码 |
| 日志 | SLF4J 全覆盖 |
| 缓存 | `activateDefaultTyping` 类型安全 + 精确驱逐 + `@CachePut` 写入预热 |
| 测试 | 15 个（7 Service 单测 + 8 Controller MockMvc 集成测试） |
| 配置 | 多环境 yml（dev）+ `${MYSQL_PASSWORD}` 环境变量 |
| 前端 | Vue 3 + Element Plus，单页面 CRUD（表格 + 弹窗表单 + 分页） |
| SQL | OR 条件用 `.and()` 包裹，`@TableLogic` 正确约束 |

**相对 v0.1.0 的 Code Review 修复（12 个问题）：**

| 严重度 | 数量 | 典型问题 |
|---|---|---|
| 🔴 必须修复 | 3 | Redis 序列化类型丢失、OR 条件绕过软删除、`updated_at` 永不更新 |
| 🟡 应该修复 | 4 | BusinessException HTTP 状态码、缓存名硬编码、`create()` 不预热缓存、死配置 |
| 🟢 可以优化 | 5 | 死代码清理、依赖冗余、SQL 日志重复、null 守卫 |

---

### v0.1.0

`git checkout v0.1.0` | [Release](https://github.com/Lotusland-Kioea/user-service-pro/releases/tag/v0.1.0)

原始极简 Demo，项目起点。

| 维度 | 内容 |
|---|---|
| 文件 | 6 个 Java 文件 |
| ORM | 原生 MyBatis 注解 SQL |
| API | 2 个（POST 创建 + GET 全量查询） |
| 分层 | Entity 直接暴露给 API |
| 校验 | 无 |
| 响应 | 裸 JSON |
| 异常 | 默认 500 + 堆栈 |
| 日志 | `System.out.println` |
| 缓存 | 未调用 `activateDefaultTyping`，反序列化类型丢失 |
| 测试 | 0 |
| 配置 | 密码明文硬编码 |
| 前端 | 无 |

---

## v1.0.0 开发过程日志

### 阶段一：项目审查与方案设计

**2026-06-03 21:09 ~ 21:30**

- 2 个 Explore Agent 并行审查原 Demo：梳理项目结构、识别代码质量问题
- 发现：6 个文件、2 个 API、零测试、零校验、`System.out.println` 日志、密码明文
- 与用户讨论技术选型：确认 MyBatis-Plus 升级、Vue 3 前端、本地 MySQL/Redis 保持
- 输出 Plan 方案，用户审批通过

### 阶段二：后端工程化

**2026-06-03 21:30 ~ 21:46**

- 3 个 Subagent 并行开发：基础类层 → 业务逻辑层 → Web 配置层
- 16 个 Java 源文件 + 2 个 yml 配置文件一次性生成
- 补充 MyBatis-Plus 分页插件和自动填充处理器
- 修复 MyBatis-Plus 3.5.9 → 3.5.7 版本兼容问题（`PaginationInnerInterceptor` 在新版被移除）
- `mvn clean test` → **15/15 通过**

### 阶段三：前端可视化

**2026-06-03 21:47 ~ 21:50**

- Vite + Vue 3 + Element Plus 脚手架搭建
- 单页面用户管理：搜索 + 分页表格 + 新增/编辑弹窗 + 删除确认
- axios 封装 + 响应拦截器 + Vite 代理
- `npm run build` 构建验证通过

### 阶段四：Code Review

**2026-06-03 21:56 ~ 22:10**

4 个 Agent 并行多角度审查（全项目无 diff，直接审查全部 18 个文件）：

| 角度 | 内容 |
|---|---|
| Angle A | 逐行扫描：逻辑错误、空指针、竞态条件 |
| Angle C | 跨文件追踪：接口契约、方法签名一致性 |
| Angle D | 语言陷阱：Spring Cache、MyBatis-Plus、Jackson、事务 |
| Angle E | 序列化深度审查：Redis 缓存层完整分析 |

**审查结果：发现 12 个问题**

| 严重度 | 数量 | 典型问题 |
|---|---|---|
| 🔴 必须修复 | 3 | Redis 序列化类型丢失、OR 条件绕过软删除、`updated_at` 永不更新 |
| 🟡 应该修复 | 4 | BusinessException HTTP 状态码、缓存名硬编码、create 不预热缓存、死配置 |
| 🟢 可以优化 | 5 | 死代码清理、依赖冗余、SQL 日志重复、null 守卫 |

### 阶段五：修复与验证

**2026-06-03 22:10 ~ 22:12**

- 4 个 Subagent 并行修复（6 个文件同时改）
- Test 适配（`ResponseEntity` 动态 HTTP 状态码 → 测试从 `isOk()` 改为 `isNotFound()`）
- `mvn clean test` → **15/15 通过，BUILD SUCCESS**

### 阶段六：运行验证

**2026-06-03 22:13 ~ 22:15**

- MySQL 表结构更新（新增 `updated_at` 和 `deleted` 字段）
- Redis `stop-writes-on-bgsave-error` 修复
- 后端 + 前端同时启动，curl 测试 CRUD 全流程通过

---

## v1.0.0 使用的 AICoding 技能

| 技能 | 使用场景 |
|---|---|
| **Plan 模式** | 方案设计、技术选型讨论、用户审批 |
| **Subagent 并行开发** | 分层代码生成（3 agent × 基础/业务/Web）、测试编写、前端全量生成 |
| **Code Review** | 4 角度并行审查（逐行扫描 + 跨文件追踪 + 语言陷阱 + 序列化深度） |
| **Explore Agent** | 原 Demo 项目审查、环境工具探测 |
| **TodoWrite** | 全流程任务追踪（9 个阶段） |
| **Bash** | Maven 编译、npm 构建、MySQL/Redis 运维、curl 验证 |

---

## Kafka 生产级知识体系（面试参考）

> 基于本项目的实际配置和验证过程，整理以下 Kafka 核心知识。每个知识点都对应到具体代码位置或实操验证。

### 一、集群高可用架构

#### KRaft vs ZooKeeper

| 维度 | ZooKeeper 模式 (Kafka 3.x 以前) | KRaft 模式 (Kafka 3.3+ / 4.0) |
|---|---|---|
| 元数据存储 | 独立 ZooKeeper 集群 | Kafka 内置 Raft 共识，`__cluster_metadata` Topic |
| 进程数 | Broker + ZooKeeper 两套 | 仅 Kafka 一套 |
| Controller 选举 | 依赖 ZK 临时节点抢注 | 内建 Raft 协议，`controller.quorum.voters` |
| 本项目配置 | — | `process.roles=broker,controller` |

#### Controller 选举

基于 Raft 协议的多数派选举。本项目 3 个 voter（`1@localhost:9093,2@localhost:9095,3@localhost:9097`），需 ≥2 票当选。Controller 负责分区 Leader 选举、ISR 管理、Broker 注册/注销。

#### 单机三节点集群

| 节点 | node.id | 客户端端口 | Controller 端口 | 数据目录 |
|---|---|---|---|---|
| 1 | 1 | 9092 | 9093 | data/kraft-1 |
| 2 | 2 | 9094 | 9095 | data/kraft-2 |
| 3 | 3 | 9096 | 9097 | data/kraft-3 |

通过不同端口和独立数据目录实现逻辑隔离。分布式协议（Raft 选举、Leader 漂移、ISR 恢复）与真正的多机集群完全一致，仅物理故障隔离不同。

#### 故障演练（本项目已验证）

| 步骤 | 现象 |
|---|---|
| 关闭节点 1 | Controller 检测心跳超时 → 分区 Leader 漂移到节点 2/3 → ISR 中移除节点 1 |
| 故障期间 | `min.insync.replicas=2` + 存活 2 节点，`acks=all` 写入正常 |
| 重启节点 1 | 作为 Follower 追赶数据 → 追平后重新纳入 ISR |

---

### 二、Topic 分区与副本

#### 核心概念

| 概念 | 说明 |
|---|---|
| **Partition** | 并行度基本单位。一个分区同一时间只被一个消费者线程消费 |
| **Replica** | 数据冗余。Leader 处理读写，Follower 被动同步 |
| **ISR** | In-Sync Replicas——与 Leader 保持同步的副本集合 |
| **HW / LEO** | High Watermark（消费者可见位置）/ Log End Offset（写入位置） |

#### 本项目 Topic 设计

| Topic | 分区 | 消费者线程 | 教学目的 |
|---|---|---|---|
| user-events | 3 | 3 | 一对一映射，最佳并行度 |
| lab-messages | 4 | 3+3 | 多对少分配，演示分区 > 线程时的分配策略 |
| user-events-dlt | 1 | — | 死信量少，单分区便于按时间线排查 |

#### min.insync.replicas 容错能力

| 存活节点 | ISR 数 | 能否写入 (acks=all) | 说明 |
|---|---|---|---|
| 3 | 3 | ✅ | 正常 |
| 2 | 2 | ✅ | 刚好满足 min.isr=2 |
| 1 | 1 | ❌ | NotEnoughReplicasException |

容忍故障节点数 = replication-factor − min.insync.replicas = 3 − 2 = **1**

---

### 三、生产者可靠性

| 配置 | 本项目值 | 作用 |
|---|---|---|
| `acks` | all | 所有 ISR 确认后返回，最高持久性 |
| `enable.idempotence` | true | Broker 通过 PID+序列号去重，分区内 exactly-once |
| `transactional.id` | user-service-pro-tx- | 跨分区事务（配合 `@Transactional`） |
| `retries` | MAX_INT | 无限重试，受 delivery.timeout.ms 约束 |
| `delivery.timeout.ms` | 120000 | 总发送窗口 120s |
| `compression.type` | snappy | CPU 与压缩率平衡 |
| `linger.ms` / `batch.size` | 5 / 16384 | 凑批发送，吞吐量 ↑ |

**acks 三档对比**：`0`=不等确认（最快、可能丢）→ `1`=Leader 确认（可能丢）→ `all`=所有 ISR 确认（最安全）

**幂等生产者原理**：Broker 分配 PID，每条消息带 `<PID, 分区, seq>`，Broker 根据 seq 去重。单分区单会话有效。

**事务生产者原理**：基于幂等，通过事务协调器两阶段提交实现跨分区 exactly-once。消费者需 `isolation.level=read_committed`。

---

### 四、消费者可靠性

| 配置 | 本项目值 | 作用 |
|---|---|---|
| `enable.auto.commit` | false | 手动提交，避免"已提交未处理"丢失 |
| `AckMode` | MANUAL | 显式 `ack.acknowledge()` |
| `isolation.level` | read_committed | 只读已提交事务消息 |
| `concurrency` | 3 | 3 线程并行消费 |
| `auto.offset.reset` | earliest | 无 offset 时从最早开始 |
| `session.timeout.ms` | 45000 | 45s 无心跳判定宕机 |
| `heartbeat.interval.ms` | 15000 | 心跳间隔（session 的 1/3） |

**手动 vs 自动提交**：自动提交可能"提交了但没处理完就宕机"→ 消息丢失；手动提交只有处理成功后才提交。

**消费者组与重平衡**：同 group 内一个分区只能被一个消费者消费。消费者数 > 分区数 → 多余线程空转。分区数 > 消费者数 → 部分线程处理多分区。Rebalance 触发条件：消费者增减、分区变化、心跳超时。

---

### 五、死信队列（DLT）

```
消费失败 → 判断异常类型 → 可重试？→ 3 次 FixedBackOff(2s) → 仍失败 → DLT
                        → 不可重试（DeserializationException）→ 直接 DLT
```

| 特性 | 实现 |
|---|---|
| DLT 命名 | `{原Topic}-dlt` |
| 退避策略 | FixedBackOff(2s × 3 次) |
| 毒丸消息 | 反序列化异常不重试，直接进 DLT |
| 元数据保留 | 原始 topic、partition、offset、异常信息存入 Header |
| 分区投递 | `partition=-1` 由 Kafka 自动选择（修复前按原分区号投递会失败） |

---

### 六、消息可靠性全景

| 层级 | 保障 |
|---|---|
| **生产端** | acks=all + 幂等 + 重试 + 事务（可选） |
| **Broker 端** | replication-factor=3 + min.insync.replicas=2 |
| **消费端** | 手动提交 + 幂等去重（eventId） |
| **兜底** | DLT 死信队列，不丢失失败消息 |

**当前模式 (v1.1.0)**：`@TransactionalEventListener(AFTER_COMMIT)` → at-least-once。DB 提交后异步发 Kafka，回调中记录失败。极端情况（DB 提交后 JVM 崩溃）消息丢失。

**演进路线**：v1.2.0 Outbox 模式（同一事务内 DB 写业务表 + outbox 表 → 定时轮询发 Kafka）→ exactly-once

---

### 面试速查

| 问题 | 一句话答案 |
|---|---|
| 如何保证不丢消息？ | 生产 acks=all + Broker replication=3 + 消费手动提交 |
| 如何保证不重复？ | 生产幂等(PID+seq) + 消费 eventId 去重 |
| 消息积压怎么办？ | 增加分区 + 增加消费者实例 + 优化处理逻辑 |
| Kafka 为什么快？ | 顺序写盘(Page Cache) + 零拷贝(sendfile) + 批量压缩 + 分区并行 |
| 为什么单机能做集群？ | node.id + 不同端口 + 独立数据目录 = 逻辑三节点 |
| Kafka 4.0 最大变化？ | 移除 ZooKeeper，KRaft 成为唯一模式 |

---

## 后续可扩展方向（未实施）

- [x] ~~Kafka 消息队列（用户注册事件异步通知）~~ ✅ v1.1.0 完成
- [ ] Spring Security + JWT 认证授权
- [ ] Docker Compose 一键部署（MySQL + Redis + Kafka + App）
- [ ] Flyway 数据库迁移管理
- [ ] MyBatis-Plus 代码生成器（自动生成 Controller/Service/Mapper）
- [ ] 前端路由 + 多页面（Vue Router）
- [ ] CI/CD（GitHub Actions 自动测试）
