package com.example.userservice.messaging;

import com.example.userservice.event.UserEvent;
import com.example.userservice.event.UserEventType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * UserEventConsumer 单元测试。
 */
@ExtendWith(MockitoExtension.class)
class UserEventConsumerTest {

    private UserEventConsumer consumer;

    @Mock
    private Acknowledgment ack;

    private UserEvent createdEvent;
    private UserEvent updatedEvent;
    private UserEvent deletedEvent;

    @BeforeEach
    void setUp() {
        consumer = new UserEventConsumer();
        createdEvent = UserEvent.of(UserEventType.CREATED, 1L, "张三", "zhangsan@example.com");
        updatedEvent = UserEvent.of(UserEventType.UPDATED, 2L, "李四", "lisi@example.com");
        deletedEvent = UserEvent.of(UserEventType.DELETED, 3L, "王五", "wangwu@example.com");
    }

    @Test
    @DisplayName("消费 CREATED 事件 - 成功确认")
    void testConsumeCreated() {
        consumer.onUserEvent(createdEvent, ack);
        verify(ack, times(1)).acknowledge();
    }

    @Test
    @DisplayName("消费 UPDATED 事件 - 成功确认")
    void testConsumeUpdated() {
        consumer.onUserEvent(updatedEvent, ack);
        verify(ack, times(1)).acknowledge();
    }

    @Test
    @DisplayName("消费 DELETED 事件 - 成功确认")
    void testConsumeDeleted() {
        consumer.onUserEvent(deletedEvent, ack);
        verify(ack, times(1)).acknowledge();
    }

    @Test
    @DisplayName("重复事件 - 幂等去重，确认但不重复处理")
    void testDuplicateEvent() {
        // 第一次消费
        consumer.onUserEvent(createdEvent, ack);
        verify(ack, times(1)).acknowledge();

        // 第二次消费相同 eventId
        consumer.onUserEvent(createdEvent, ack);
        // 仍然确认（避免反复重试），但日志会输出"重复事件"
        verify(ack, times(2)).acknowledge();
    }
}
