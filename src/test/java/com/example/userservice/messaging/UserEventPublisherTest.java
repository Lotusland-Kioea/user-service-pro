package com.example.userservice.messaging;

import com.example.userservice.event.UserEvent;
import com.example.userservice.event.UserEventType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.quality.Strictness;
import org.mockito.junit.jupiter.MockitoSettings;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * UserEventPublisher 单元测试。
 * <p>
 * 注意：JDK 26 + mock-maker-subclass 可 Mock KafkaTemplate，
 * 但 send() 返回 null 需 stub 为未完成的 CompletableFuture 避免 NPE。
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class UserEventPublisherTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    private UserEventPublisher publisher;

    private UserEvent testEvent;

    @BeforeEach
    void setUp() {
        // stub send() 返回未完成的 Future，防止 callback 中的 NPE
        // send(String topic, K key, V data) — 3 参数版本
        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(new CompletableFuture<>());

        publisher = new UserEventPublisher(kafkaTemplate);
        testEvent = UserEvent.of(UserEventType.CREATED, 1L, "张三", "zhangsan@example.com");
    }

    @Test
    @DisplayName("异步发送 - 调用 kafkaTemplate.send")
    void testSendAsync() {
        publisher.sendAsync(testEvent);
        verify(kafkaTemplate, times(1))
                .send(eq("user-events"), eq(testEvent.getEventId()), eq(testEvent));
    }

    @Test
    @DisplayName("带 Key 发送 - 以 userId 为 Key")
    void testSendWithKey() {
        publisher.sendWithKey(testEvent);

        String expectedKey = String.valueOf(testEvent.getUserId());
        verify(kafkaTemplate, times(1))
                .send(eq("user-events"), eq(expectedKey), eq(testEvent));
    }

    @Test
    @DisplayName("事件中包含完整信息")
    void testEventFields() {
        assertNotNull(testEvent.getEventId());
        assertEquals(UserEventType.CREATED, testEvent.getEventType());
        assertEquals(1L, testEvent.getUserId());
        assertEquals("张三", testEvent.getUserName());
        assertEquals("user-service", testEvent.getSource());
    }
}
