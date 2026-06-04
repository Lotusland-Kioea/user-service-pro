package com.example.userservice.common;

/**
 * Kafka Topic 名称常量。
 * <p>
 * 沿用 {@link CacheConstants} 的设计风格，集中管理 Topic 名称，
 * 避免硬编码字符串分散在各处。
 *
 * @since 1.1.0
 */
public final class KafkaTopicConstants {

    private KafkaTopicConstants() {
        // 工具类，禁止实例化
    }

    /** 用户事件 Topic（创建/更新/删除时发布） */
    public static final String USER_EVENTS = "user-events";

    /** 用户事件死信队列（消费失败重试耗尽后转入） */
    public static final String USER_EVENTS_DLT = "user-events-dlt";

    /** 实验室消息 Topic（KafkaLab 学习用） */
    public static final String LAB_MESSAGES = "lab-messages";

    /** 实验室死信队列 */
    public static final String LAB_MESSAGES_DLT = "lab-messages-dlt";
}
