# user-service

Spring Boot 极简用户管理 API，技术栈：**Java 26 + Maven + Spring Boot + MyBatis + MySQL + Redis**。

## 环境信息

| 工具 | 版本 | 路径 |
|---|---|---|
| Java | 26.0.1 (Temurin, 编译目标 21) | `D:\Develop\Java\jdk-26.0.1+8\` |
| Maven | 3.9.16 | `D:\Develop\Maven\` |
| MySQL | 8.0.43 | `D:\Develop\MySQL\` |
| Redis | 5.0.14.1 (tporadowski) | `D:\Develop\Redis\` |
| IntelliJ IDEA | 2025.2.6.2 | `D:\Develop\JetBrains\IntelliJ IDEA\` |

## 快速启动

### 1. 确认 MySQL 和 Redis 已启动
```bash
sc query MySQL | findstr RUNNING
sc query Redis | findstr RUNNING
```
如果没启动，用管理员 CMD 执行 `sc start MySQL` 和 `sc start Redis`。

### 2. 建表（只需一次）
```bash
mysql -u root -p你的密码 mydb
```
```sql
CREATE TABLE IF NOT EXISTS users (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(200) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### 3. 编译 + 启动
```bash
cd E:\Project\user-service
mvn spring-boot:run
```
看到 "user-service 启动完成!" 就表示成功了。

### 4. 测试接口
打开**新的 CMD 窗口**（不要关 IDEA），执行：
```bash
# 创建用户
curl -X POST http://localhost:8080/users -H "Content-Type: application/json" -d "{\"name\":\"Alice\",\"email\":\"alice@example.com\"}"

# 查询所有用户（首次查 MySQL，再次查 Redis 缓存）
curl http://localhost:8080/users

# 查看 Redis 缓存键（注意：如 redis-cli 提示"拒绝访问"，改用 PowerShell 执行）
redis-cli KEYS "*"
```
> **Windows 注意**：`redis-cli.exe` 如果直接运行报"拒绝访问"，用 PowerShell 执行即可绕开。

### 5. 如果 Git Bash 里找不到 mvn/mysql/redis-cli
这是因为 Git Bash 不会自动继承 Windows 系统 PATH 中新增的条目。已通过 `~/.bashrc` 修复。如果仍然找不到，关闭重新打开 Git Bash 窗口即可。

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/users` | 创建用户，请求体 `{"name":"...", "email":"..."}`，返回插入后的完整记录 |
| GET | `/users` | 查询所有用户，Redis 缓存 60 秒，写入时自动清缓存 |

## 缓存逻辑

- **POST** → 写 MySQL → `@CacheEvict` 删除 `users:all` 缓存
- **GET** → `@Cacheable` 先查 Redis → 命中直接返回 → 未命中查 MySQL → 写入 Redis（TTL 60s）
- 控制台有 `[Service]` `[POST]` `[GET]` 标记的 println 输出，可以直观看到缓存命中/未命中

## 项目结构

```
user-service/
├── pom.xml                              (依赖：Spring Boot 3.4.5 + MyBatis + Redis)
├── README.md
└── src/main/
    ├── java/com/example/demo/
    │   ├── DemoApplication.java          (启动类，@EnableCaching)
    │   ├── entity/User.java              (实体类，对应 users 表)
    │   ├── mapper/UserMapper.java        (MyBatis 接口：@Insert + @Select)
    │   ├── service/UserService.java      (业务层：@Cacheable + @CacheEvict)
    │   ├── controller/UserController.java (REST 接口层)
    │   └── config/RedisConfig.java       (Redis 配置：Jackson JSON 序列化)
    └── resources/
        └── application.yml               (MySQL/Redis 连接配置)
```

## 配置说明

- **MySQL**: `root` / `你的密码`，数据库 `mydb`，端口 `3306`
- **Redis**: `localhost:6379`，无密码
- **应用端口**: `8080`
- **Maven 镜像**: 已配置阿里云（`%USERPROFILE%\.m2\settings.xml`）
- **Redis 序列化**: Jackson JSON（不是 Java 默认序列化，值可读）
## 常见问题

| 现象 | 原因 | 解决 |
|---|---|---|
| 端口 8080 被占用 | 上次未正常关闭 | `netstat -ano \| findstr :8080` 找到 PID，`taskkill /F /PID 数字` |
| Access denied for user | MySQL 密码错误或服务未启动 | 管理员 CMD: `sc start MySQL`，密码为你的MySQL密码 |
| Redis connection refused | Redis 服务未启动 | 管理员 CMD: `sc start Redis` |
| redis-cli.exe 拒绝访问 | CMD 安全策略拦截 | 用 **PowerShell** 执行 |
| mvn 命令找不到 | PATH 未包含 Maven | 重启终端，或 `export PATH="$PATH:/d/Develop/Maven/bin"` |
| curl 不是内部命令 | 旧版 Windows 不自带 curl | 用 Git Bash（自带 curl） |
| 启动报 ASM/CGLIB 错误 | Java 太新不兼容 | 回退 Java 21，切换 JAVA_HOME |
| D 盘目录打不开 | Redis 安装改变了 ACL | 管理员 CMD: `icacls D:\ /grant "14032:(OI)(CI)F" /T` |
