package com.example.userservice.lab;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.example.userservice.common.KafkaTopicConstants.LAB_MESSAGES;

/**
 * Kafka 实验室 — 消费者。
 *
 * <p>演示两种消费模式：
 * <ul>
 *   <li><b>单条消费</b>：逐条处理并手动提交 offset（使用默认 containerFactory）</li>
 *   <li><b>批量消费</b>：一次拉取多条消息，处理完一批后统一提交
 *       （使用 {@code batchListenerContainerFactory}）</li>
 * </ul>
 *
 * <p>同时演示可配置的失败概率（{@code kafka.demo.fail-chance}），
 * 用于触发重试和 DLT 流程。设置为 1.0 时每条消息都会"处理失败"，
 * 经过 3 次重试后被转发到 {@code lab-messages-dlt}。
 *
 * @since 1.1.0
 */
@Component
public class KafkaLabConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaLabConsumer.class);

    private final Random random = new Random();
    private final AtomicLong consumedCount = new AtomicLong(0);

    /**
     * 模拟失败概率（0.0 ~ 1.0），由 application-kafka.yml 配置。
     */
    @Value("${kafka.demo.fail-chance:0.0}")
    private double failChance;

    /**
     * ★ 单条消费模式 — 使用默认 kafkaListenerContainerFactory。
     *
     * <p>每条消息独立处理，手动提交 offset。
     * 如果随机失败（按 failChance 概率），抛出异常触发重试。
     */
    @KafkaListener(
            topics = LAB_MESSAGES,
            groupId = "lab-consumer-group"
    )
    public void onMessage(
            @Payload LabMessage message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {

        long count = consumedCount.incrementAndGet();

        log.info("[Lab-Consumer-单条] #{}: partition={}, offset={}, id={}, key={}, payload={}",
                count, partition, offset, message.getId(), message.getKey(), message.getPayload());

        // ★ 演示点：按配置的概率模拟消费失败
        if (random.nextDouble() < failChance) {
            log.warn("[Lab-Consumer-单条] 模拟处理失败! id={}, failChance={}", message.getId(), failChance);
            throw new RuntimeException("模拟消费失败: id=" + message.getId());
        }

        // 处理成功，手动确认
        ack.acknowledge();
    }

    /**
     * ★ 批量消费模式 — 使用 batchListenerContainerFactory。
     *
     * <p>一次拉取多条消息，处理完整个批次后统一提交。
     * 适合需要批量写入数据库、批量调用外部 API 等场景。
     *
     * <p>通过 {@code containerFactory = "batchListenerContainerFactory"} 指定使用
     * {@code KafkaConfig} 中配置的批量工厂。
     */
    @KafkaListener(
            topics = LAB_MESSAGES,
            groupId = "lab-consumer-group-batch",
            containerFactory = "batchListenerContainerFactory"
    )
    public void onMessageBatch(
            @Payload List<LabMessage> messages,
            Acknowledgment ack) {

        log.info("[Lab-Consumer-批量] 收到 {} 条消息", messages.size());

        for (LabMessage msg : messages) {
            log.info("[Lab-Consumer-批量]   id={}, key={}, payload={}",
                    msg.getId(), msg.getKey(), msg.getPayload());
        }

        // 批量处理完成，统一提交
        ack.acknowledge();
        log.info("[Lab-Consumer-批量] 批量确认完成，共 {} 条", messages.size());
    }

    /**
     * 获取已消费消息计数。
     */
    public long getConsumedCount() {
        return consumedCount.get();
    }
}
