package com.example.userservice.config;

import com.example.userservice.common.KafkaTopicConstants;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Kafka Topic 声明式配置。
 * <p>
 * 通过 {@link NewTopic} Bean 在应用启动时自动创建 Topic，
 * 无需手动执行 {@code kafka-topics.bat --create}。
 * 如果 Topic 已存在，不会重复创建。
 * <p>
 * 分区策略：
 * <ul>
 *   <li><b>user-events</b>：3 分区，对应 3 个消费者线程并发处理</li>
 *   <li><b>user-events-dlt</b>：1 分区，死信消息量少，无需高并发</li>
 *   <li><b>lab-messages</b>：4 分区，故意多于消费者线程数，
 *       用于演示分区分配策略（一个消费者处理多个分区）</li>
 *   <li><b>lab-messages-dlt</b>：1 分区</li>
 * </ul>
 *
 * @since 1.1.0
 */
@Configuration
public class KafkaTopicConfig {

    /**
     * 用户事件 Topic — 3 分区 3 副本。
     * <p>
     * 3 分区对应 3 个并发消费者线程，每个线程独占一个分区。
     * 以 userId 为 Key，保证同一用户的事件有序。
     */
    @Bean
    public NewTopic userEventsTopic() {
        return TopicBuilder.name(KafkaTopicConstants.USER_EVENTS)
                .partitions(3)
                .replicas(3)
                .build();
    }

    /**
     * 用户事件死信队列 — 1 分区 3 副本。
     * <p>
     * 死信消息通常需要人工介入排查，不需要高吞吐量。
     */
    @Bean
    public NewTopic userEventsDltTopic() {
        return TopicBuilder.name(KafkaTopicConstants.USER_EVENTS_DLT)
                .partitions(1)
                .replicas(3)
                .build();
    }

    /**
     * 实验室消息 Topic — 4 分区 3 副本。
     * <p>
     * 4 分区 &gt; 3 消费者线程，演示多对多的分区分配策略。
     * 通过 {@code POST /kafka-lab/send-with-key/{key}} 发送消息时，
     * 相同 Key 总是路由到同一分区，可在日志中验证。
     */
    @Bean
    public NewTopic labMessagesTopic() {
        return TopicBuilder.name(KafkaTopicConstants.LAB_MESSAGES)
                .partitions(4)
                .replicas(3)
                .build();
    }

    /**
     * 实验室死信队列 — 1 分区 3 副本。
     */
    @Bean
    public NewTopic labMessagesDltTopic() {
        return TopicBuilder.name(KafkaTopicConstants.LAB_MESSAGES_DLT)
                .partitions(1)
                .replicas(3)
                .build();
    }
}
