package com.example.userservice.messaging;

import com.example.userservice.event.UserEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.example.userservice.common.KafkaTopicConstants.USER_EVENTS;

/**
 * 用户事件 Kafka 生产者。
 *
 * <h3>三种发送模式演示</h3>
 * <ul>
 *   <li><b>异步发送（推荐）</b>：{@link #sendAsync(UserEvent)} —
 *     不阻塞调用线程，通过回调处理结果，吞吐量最高</li>
 *   <li><b>同步发送</b>：{@link #sendSync(UserEvent)} —
 *     等待确认后返回，适合必须知道结果的场景，但会阻塞线程</li>
 *   <li><b>带 Key 的异步发送</b>：{@link #sendWithKey(UserEvent)} —
 *     以 userId 为 Key，同一用户的事件始终路由到同一分区，保证有序</li>
 * </ul>
 *
 * <h3>生产环境注意事项</h3>
 * <ul>
 *   <li>异步发送的 callback 中不要抛异常，否则框架吞掉后续回调</li>
 *   <li>同步发送需要设置超时，防止永久阻塞线程</li>
 *   <li>Key 的选择影响分区分布——热点用户可能造成分区倾斜</li>
 * </ul>
 *
 * @since 1.1.0
 */
@Component
public class UserEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(UserEventPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public UserEventPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * ★ 异步发送（生产环境最常用）。
     *
     * <p>消息入队后立即返回，不等待 broker 确认。
     * 通过 CompletableFuture 的 whenComplete 处理成功/失败。
     *
     * <p>吞吐量最高的方式，适合大多数业务场景。
     *
     * @param event 用户事件
     */
    public void sendAsync(UserEvent event) {
        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(USER_EVENTS, event.getEventId(), event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("用户事件发送成功: eventId={}, topic={}, partition={}, offset={}",
                        event.getEventId(),
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("用户事件发送失败: eventId={}, userId={}, error={}",
                        event.getEventId(), event.getUserId(), ex.getMessage());
            }
        });
    }

    /**
     * ★ 同步发送 — 等待 broker 确认后返回结果。
     *
     * <p>调用线程会阻塞，直到收到确认或超时。
     * 适合需要立即知道发送结果的场景（如 API 响应中返回 offset）。
     *
     * @param event   用户事件
     * @param timeout 超时秒数
     * @return 发送结果（含 partition、offset），超时则返回 null
     */
    public SendResult<String, Object> sendSync(UserEvent event, long timeout) {
        try {
            CompletableFuture<SendResult<String, Object>> future =
                    kafkaTemplate.send(USER_EVENTS, event.getEventId(), event);
            SendResult<String, Object> result = future.get(timeout, TimeUnit.SECONDS);
            log.info("用户事件同步发送成功: eventId={}, partition={}, offset={}",
                    event.getEventId(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
            return result;
        } catch (Exception e) {
            log.error("用户事件同步发送超时或失败: eventId={}, error={}",
                    event.getEventId(), e.getMessage());
            return null;
        }
    }

    /**
     * ★ 带 Key 发送 — 以 userId 为 Key，保证同一用户事件有序。
     *
     * <p>Kafka 根据 Key 的哈希值选择分区，相同 Key 总是路由到同一分区。
     * 这意味着同一用户的事件按发送顺序写入，Consumer 按序消费。
     *
     * <p><b>场景</b>：用户先改名、再改邮箱，Consumer 必须按顺序处理，
     * 否则可能用旧名字覆盖新名字。
     *
     * @param event 用户事件（eventId 作为消息 Key）
     */
    public void sendWithKey(UserEvent event) {
        // 以 userId 为 Key，实现分区亲和性：同一用户的事件进入同一分区
        String key = String.valueOf(event.getUserId());

        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(USER_EVENTS, key, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("用户事件(Key={})发送成功: eventId={}, partition={}, offset={}",
                        key, event.getEventId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("用户事件(Key={})发送失败: eventId={}, error={}",
                        key, event.getEventId(), ex.getMessage());
            }
        });
    }
}
