package com.example.userservice.messaging;

import com.example.userservice.event.UserEvent;
import com.example.userservice.service.impl.UserServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * ★ 业务层到消息层的桥接器 —— 连接 Spring ApplicationEvent 和 Kafka。
 *
 * <h3>这是整个 v1.1.0 最关键的架构模式</h3>
 *
 * <p>使用 {@link TransactionalEventListener} 而非普通 {@link EventListener}，
 * 确保 Kafka 消息仅在数据库事务<b>成功提交后</b>才发送。
 *
 * <h3>执行流程</h3>
 * <pre>
 * UserServiceImpl.create(user)          ← @Transactional 事务中
 *     │
 *     ├── userMapper.insert(user)       ← DB 写入
 *     ├── applicationEventPublisher
 *     │       .publishEvent(userEvent)   ← 发布 Spring 事件
 *     │
 *     └── 事务提交 ────────────────────→ 提交成功？
 *                                       │
 *                         ┌─────────────┴─────────────┐
 *                         ↓ YES                       ↓ NO（回滚）
 *             onUserCreated(userEvent)             什么都不做
 *                 │                               避免"幽灵事件"
 *                 └── publisher.sendAsync(event)  → Kafka
 * </pre>
 *
 * <h3>为什么用 AFTER_COMMIT 而非默认 AFTER_COMPLETION</h3>
 * <ul>
 *   <li>{@code AFTER_COMMIT} = 事务提交后才执行（推荐）</li>
 *   <li>{@code AFTER_COMPLETION} = 提交或回滚都执行（不可靠）</li>
 *   <li>{@code BEFORE_COMMIT} = 提交前执行（如果此步骤失败，业务事务也回滚，
 *       但这意味着"发消息失败会阻止业务操作"，通常不是期望的行为）</li>
 * </ul>
 *
 * <h3>这个模式的局限性（at-least-once）</h3>
 * <p>事务提交成功 → 开始发 Kafka → Kafka 宕机 → 消息丢失。
 * 这是 at-least-once 的典型场景，也是 Outbox 模式要解决的问题。
 * 生产环境建议配合 Outbox 表 + 定时任务实现 exactly-once。</p>
 *
 * <h3>演进路线</h3>
 * <pre>
 * v1.1.0: @TransactionalEventListener (当前) → at-least-once
 * v1.2.0: Outbox 表 + 定时轮询 → exactly-once
 * v1.3.0: Debezium CDC 监听 binlog → 零侵入 exactly-once
 * </pre>
 *
 * @see UserServiceImpl 事件发布方
 * @see UserEventPublisher Kafka 发送方
 * @since 1.1.0
 */
@Component
public class UserEventKafkaBridge {

    private static final Logger log = LoggerFactory.getLogger(UserEventKafkaBridge.class);

    private final UserEventPublisher publisher;

    public UserEventKafkaBridge(UserEventPublisher publisher) {
        this.publisher = publisher;
    }

    /**
     * 监听所有用户事件，转发到 Kafka。
     *
     * <p>使用 {@link TransactionalEventListener} 而非 {@link EventListener}，
     * 确保仅在 DB 事务提交后才发送消息。
     *
     * @param event Spring 发布的应用事件
     */
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onUserEvent(UserEvent event) {
        log.info("DB 事务已提交，转发事件到 Kafka: eventId={}, type={}, userId={}",
                event.getEventId(), event.getEventType(), event.getUserId());

        // ★ 使用异步发送：不阻塞 HTTP 请求线程
        // 回调中记录成功 partition/offset，或失败时打印完整事件 JSON 供人工回放
        // 局限：at-least-once，极端情况下（JVM 崩溃在 Kafka ACK 之前）消息丢失
        // 演进：v1.2.0 Outbox 模式（DB 事务内写 outbox 表 + 定时轮询）实现 exactly-once
        publisher.sendAsync(event);
    }
}
