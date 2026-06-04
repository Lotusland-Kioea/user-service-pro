package com.example.userservice.messaging;

import com.example.userservice.event.UserEvent;
import com.example.userservice.event.UserEventType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * UserEventKafkaBridge 单元测试。
 */
@ExtendWith(MockitoExtension.class)
class UserEventKafkaBridgeTest {

    @Mock
    private UserEventPublisher publisher;

    private UserEventKafkaBridge bridge;

    private UserEvent testEvent;

    @BeforeEach
    void setUp() {
        bridge = new UserEventKafkaBridge(publisher);
        testEvent = UserEvent.of(UserEventType.CREATED, 1L, "张三", "zhangsan@example.com");
    }

    @Test
    @DisplayName("Spring 事件触发后异步转发到 KafkaPublisher")
    void testBridgeForwardsEvent() {
        bridge.onUserEvent(testEvent);
        verify(publisher, times(1)).sendAsync(testEvent);
    }

    @Test
    @DisplayName("事件转发不吞异常")
    void testBridgeWithNull() {
        bridge.onUserEvent(testEvent);
        verify(publisher, times(1)).sendAsync(any(UserEvent.class));
    }
}
