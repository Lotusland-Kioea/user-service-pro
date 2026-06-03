# user-service-pro

基于 Spring Boot 3.4.5 + MyBatis-Plus + Redis + Vue 3 的用户管理服务，从极简 Demo 重构为工程化脚手架。

---

## 开发背景

本项目是 **AI 辅助编程（AICoding）学习实践**，从一个仅 6 个 Java 文件、200 行代码的 Spring Boot 极简 Demo 出发，通过 Claude Code 的 subagent 并行开发、Plan 模式设计、Code Review 审查等技能，完成了一次完整的"规范化 → 可视化 → 审查修复"迭代流程。

### 学习目标

- 搭建规范的 Spring Boot 后端工程骨架（分层、校验、异常、日志、测试）
- 跑通单机 MySQL + Redis 缓存，理解 MyBatis-Plus 的 ORM 能力
- 搭建 Vue 3 + Element Plus 前端，实现前后端联调
- 熟悉 Claude Code 开发流程：Plan（方案设计）→ Subagent（并行开发）→ Code Review（审查修复）
- 体验从 Demo 到可交付项目的完整重构过程

---

## 技术栈

| 层级 | 技术 | 版本 |
|---|---|---|
| 后端框架 | Spring Boot | 3.4.5 |
| ORM | MyBatis-Plus | 3.5.7 |
| 数据库 | MySQL | 8.x |
| 缓存 | Redis | Windows build |
| 前端 | Vue 3 + Vite | 3.x |
| UI 库 | Element Plus | latest |
| HTTP 客户端 | axios | latest |
| 测试 | JUnit 5 + Mockito + MockMvc | — |

---

## 本地环境

| 工具 | 路径 | 版本 |
|---|---|---|
| Java | `D:\Develop\Java\jdk-26.0.1+8` | OpenJDK 26 |
| Maven | `D:\Develop\Maven` | — |
| Node.js | `D:\Develop\Nodejs` | v24.16.0 |
| MySQL | `D:\Develop\MySQL` | 8.x |
| Redis | `D:\Develop\Redis` | 5.0.14 (Windows) |
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
│   │   └── CacheConstants.java           ← 缓存 key 常量
│   ├── config/
│   │   ├── MyBatisPlusConfig.java        ← 分页插件 + 自动填充
│   │   ├── RedisConfig.java              ← Redis 缓存配置
│   │   └── WebConfig.java                ← CORS 跨域
│   ├── controller/
│   │   └── UserController.java           ← REST 控制器
│   ├── dto/
│   │   ├── CreateUserRequest.java        ← 创建请求 DTO
│   │   └── UpdateUserRequest.java        ← 更新请求 DTO
│   ├── entity/
│   │   └── User.java                     ← 数据库实体
│   ├── exception/
│   │   ├── BusinessException.java        ← 业务异常
│   │   └── GlobalExceptionHandler.java   ← 全局异常处理
│   ├── mapper/
│   │   └── UserMapper.java               ← MyBatis-Plus Mapper
│   ├── service/
│   │   ├── UserService.java              ← 服务接口
│   │   └── impl/UserServiceImpl.java     ← 服务实现（含缓存）
│   └── vo/
│       └── UserVO.java                   ← 视图对象
├── src/main/resources/
│   ├── application.yml                   ← 公共配置
│   └── application-dev.yml               ← 开发环境配置
├── src/test/java/com/example/userservice/
│   ├── controller/
│   │   └── UserControllerTest.java       ← Controller 集成测试（8 个）
│   └── service/
│       └── UserServiceTest.java          ← Service 单元测试（7 个）
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
  user-service-pro 启动成功!
  API 地址: http://localhost:8080/users
  数据库:   localhost:3306/mydb
  Redis:    localhost:6379
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
# 预期：Tests run: 15, Failures: 0, Errors: 0, Skipped: 0
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

| 版本 | Tag | 说明 |
|---|---|---|
| **v0.1.0** | `git checkout v0.1.0` | 原始 Demo：Spring Boot + MyBatis + MySQL + Redis，2 个 API（POST/GET），6 个 Java 文件，零测试零校验 |
| **v1.0.0** | `git checkout v1.0.0` | 工程化版本：MyBatis-Plus 升级，完整 CRUD + 分页搜索，DTO 分层 + 校验 + 全局异常 + SLF4J 日志，Redis 缓存安全序列化，15 个测试，Vue 3 + Element Plus 前端 |

### v0.1.0 → v1.0.0 主要变更

| 维度 | v0.1.0 | v1.0.0 |
|---|---|---|
| 文件数 | 6 个 Java | 16 个 Java + 2 个测试类 |
| ORM | 原生 MyBatis 注解 SQL | MyBatis-Plus（BaseMapper + 分页 + 逻辑删除 + 自动填充） |
| API | 2 个（POST + GET 全量） | 5 个（完整 CRUD + 分页搜索） |
| 分层 | Entity 直接暴露 | DTO → Entity → VO 三层解耦 |
| 校验 | 无 | `@Valid` + Jakarta Bean Validation |
| 响应 | 裸 JSON | 统一 `ApiResponse<T>` 包装 |
| 异常 | 500 + 堆栈 | `@RestControllerAdvice` + 动态 HTTP 状态码 |
| 日志 | `System.out.println` | SLF4J 全覆盖 |
| 缓存 | 忘记调用 `activateDefaultTyping`，反序列化类型丢失 | 已修复类型丢失 + 精确驱逐 + 写入预热 |
| 测试 | 0 | **15 个**（7 单测 + 8 集成测试） |
| 配置 | 密码明文硬编码 | 环境变量 + 多环境 yml |
| 前端 | 无 | Vue 3 + Element Plus |
| SQL 安全 | OR 条件绕过 `deleted=0` | 已修复（`and()` 包裹） |

---

## 开发过程日志

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

## AICoding 技能清单（本次实践）

| 技能 | 使用场景 |
|---|---|
| **Plan 模式** | 方案设计、技术选型讨论、用户审批 |
| **Subagent 并行开发** | 分层代码生成（3 agent × 基础/业务/Web）、测试编写、前端全量生成 |
| **Code Review** | 4 角度并行审查（逐行扫描 + 跨文件追踪 + 语言陷阱 + 序列化深度） |
| **Explore Agent** | 原 Demo 项目审查、环境工具探测 |
| **TodoWrite** | 全流程任务追踪（9 个阶段） |
| **Bash** | Maven 编译、npm 构建、MySQL/Redis 运维、curl 验证 |

---

## 后续可扩展方向（未实施）

- [ ] Kafka 消息队列（用户注册事件异步通知）
- [ ] Spring Security + JWT 认证授权
- [ ] Docker Compose 一键部署（MySQL + Redis + Kafka + App）
- [ ] Flyway 数据库迁移管理
- [ ] MyBatis-Plus 代码生成器（自动生成 Controller/Service/Mapper）
- [ ] 前端路由 + 多页面（Vue Router）
- [ ] CI/CD（GitHub Actions 自动测试）
