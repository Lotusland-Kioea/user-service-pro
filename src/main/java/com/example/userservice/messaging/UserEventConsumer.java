package com.example.userservice.messaging;

import com.example.userservice.event.UserEvent;
import com.example.userservice.event.UserEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.example.userservice.common.KafkaTopicConstants.USER_EVENTS;

/**
 * 用户事件 Kafka 消费者。
 *
 * <h3>核心教育点</h3>
 * <ul>
 *   <li><b>手动提交偏移量</b>：通过 {@link Acknowledgment#acknowledge()} 在消息处理成功后
 *     手动提交 offset。如果处理失败，不提交 offset 即可实现自动重试</li>
 *   <li><b>幂等去重</b>：消费端通过 eventId + 内存 Map 去重（教育用途）。
 *     生产环境建议使用 Redis {@code SET key NX EX} 或数据库唯一约束</li>
 *   <li><b>按事件类型分发</b>：根据 {@link UserEventType} 做不同的业务处理</li>
 * </ul>
 *
 * <h3>生产环境演进方向</h3>
 * <ol>
 *   <li>幂等：内存 Map → Redis (SETNX + TTL)</li>
 *   <li>去重时长：内存永不过期 → Redis TTL 7天 + 定期清理</li>
 *   <li>消费者组：单实例 → 多实例共享 group-id，分区自动分配</li>
 * </ol>
 *
 * @since 1.1.0
 */
@Component
public class UserEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(UserEventConsumer.class);

    /**
     * ★ 已处理事件 ID 的内存缓存（教育用途）。
     *
     * <p>生产环境替代方案：
     * <pre>{@code
     *   // Redis: SET event:{eventId} 1 EX 604800 NX  → 7天过期
     *   // DB: INSERT INTO processed_events (event_id) VALUES (?) ON DUPLICATE KEY
     * }</pre>
     */
    private final Map<String, Boolean> processedEvents = new ConcurrentHashMap<>();

    /**
     * 消费用户事件。
     *
     * <p>监听 {@value USER_EVENTS} Topic，手动确认 offset。
     *
     * @param event     反序列化后的事件对象
     * @param ack       手动确认对象
     */
    @KafkaListener(
            topics = USER_EVENTS,
            groupId = "user-service-group"
    )
    public void onUserEvent(
            @Payload UserEvent event,
            Acknowledgment ack) {

        log.info("=== 收到用户事件 === eventId={}, type={}, userId={}, name={}",
                event.getEventId(), event.getEventType(),
                event.getUserId(), event.getUserName());

        // ---- 步骤 1：幂等去重 ----
        if (processedEvents.containsKey(event.getEventId())) {
            log.warn("重复事件，跳过: eventId={}", event.getEventId());
            ack.acknowledge();
            return;
        }

        // ---- 步骤 2：按事件类型分发处理 ----
        try {
            switch (event.getEventType()) {
                case CREATED:
                    handleUserCreated(event);
                    break;
                case UPDATED:
                    handleUserUpdated(event);
                    break;
                case DELETED:
                    handleUserDeleted(event);
                    break;
                default:
                    log.warn("未知事件类型: {}", event.getEventType());
            }

            processedEvents.put(event.getEventId(), true);
            ack.acknowledge();
            log.info("事件处理完成并确认: eventId={}", event.getEventId());

        } catch (Exception e) {
            log.error("事件处理失败: eventId={}, error={}", event.getEventId(), e.getMessage());
            throw e;
        }
    }

    /**
     * 处理"用户创建"事件。
     *
     * <p>实际项目中此处可以：发送欢迎邮件、创建默认配置、通知其他系统等。
     */
    private void handleUserCreated(UserEvent event) {
        log.info("[用户已创建] userId={}, name={}, email={}",
                event.getUserId(), event.getUserName(), event.getUserEmail());
        // TODO: 实际业务逻辑
    }

    /**
     * 处理"用户更新"事件。
     */
    private void handleUserUpdated(UserEvent event) {
        log.info("[用户已更新] userId={}, name={}, email={}",
                event.getUserId(), event.getUserName(), event.getUserEmail());
        // TODO: 同步到搜索引擎、清理缓存等
    }

    /**
     * 处理"用户删除"事件。
     */
    private void handleUserDeleted(UserEvent event) {
        log.info("[用户已删除] userId={}, name={}",
                event.getUserId(), event.getUserName());
        // TODO: 清理关联数据、通知下游
    }
}
