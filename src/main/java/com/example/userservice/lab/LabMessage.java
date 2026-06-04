package com.example.userservice.lab;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Kafka 实验室消息 — 简单 DTO。
 *
 * <p>用于 /kafka-lab/* 端点的请求和响应体，
 * 演示 JSON 序列化/反序列化、消息 Key、分区路由等概念。
 *
 * @since 1.1.0
 */
public class LabMessage {

    private String id;
    private String key;
    private String payload;
    private LocalDateTime timestamp;

    public LabMessage() {
    }

    public static LabMessage of(String key, String payload) {
        LabMessage msg = new LabMessage();
        msg.id = UUID.randomUUID().toString().substring(0, 8);
        msg.key = key;
        msg.payload = payload;
        msg.timestamp = LocalDateTime.now();
        return msg;
    }

    // ======== Getters and Setters ========

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }
}
