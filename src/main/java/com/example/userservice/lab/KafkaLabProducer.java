package com.example.userservice.lab;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.example.userservice.common.KafkaTopicConstants.LAB_MESSAGES;

/**
 * Kafka 实验室 — 生产者。
 *
 * <p>演示 KafkaTemplate 的各种高级用法：
 * <ul>
 *   <li>异步发送 + 回调</li>
 *   <li>同步发送 + 超时</li>
 *   <li>指定 Key（分区路由）</li>
 *   <li>事务发送（exactly-once）</li>
 *   <li>批量发送</li>
 * </ul>
 *
 * <p>每个方法都返回详细的元数据（topic、partition、offset），
 * 供 {@link KafkaLabController} 展示给学习者。
 *
 * @since 1.1.0
 */
@Component
public class KafkaLabProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaLabProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    /** 成功计数器 */
    private final AtomicLong successCount = new AtomicLong(0);
    /** 失败计数器 */
    private final AtomicLong failureCount = new AtomicLong(0);

    public KafkaLabProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * ★ 异步发送（演示 ListenableFuture 回调模式）。
     *
     * <p>返回值中的 partition/offset 为 -1 表示尚未确认（异步未返回），
     * 实际结果在日志中查看。
     */
    public LabSendResult sendAsync(LabMessage message) {
        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(LAB_MESSAGES, message.getKey(), message);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                successCount.incrementAndGet();
                log.info("[Lab-Async] 发送成功: id={}, partition={}, offset={}",
                        message.getId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                failureCount.incrementAndGet();
                log.error("[Lab-Async] 发送失败: id={}, error={}",
                        message.getId(), ex.getMessage());
            }
        });

        return LabSendResult.of(message, -1, -1, "async");
    }

    /**
     * ★ 同步发送（演示阻塞等待确认）。
     */
    public LabSendResult sendSync(LabMessage message, long timeoutSec) {
        try {
            CompletableFuture<SendResult<String, Object>> future =
                    kafkaTemplate.send(LAB_MESSAGES, message.getKey(), message);
            SendResult<String, Object> result = future.get(timeoutSec, TimeUnit.SECONDS);
            successCount.incrementAndGet();

            int partition = result.getRecordMetadata().partition();
            long offset = result.getRecordMetadata().offset();

            log.info("[Lab-Sync] 发送成功: id={}, partition={}, offset={}",
                    message.getId(), partition, offset);
            return LabSendResult.of(message, partition, offset, "sync");
        } catch (Exception e) {
            failureCount.incrementAndGet();
            log.error("[Lab-Sync] 发送失败: id={}, error={}", message.getId(), e.getMessage());
            return LabSendResult.of(message, -1, -1, "sync-failed: " + e.getMessage());
        }
    }

    /**
     * ★ 批量发送 — 在循环中连续发送 N 条消息。
     *
     * <p>演示 linger.ms 和 batch.size 的批量效果：
     * 如果 linger.ms=5，每 5ms 内的消息会打包成一个 batch 发送，
     * 减少网络往返次数。
     */
    public long sendBatch(LabMessage message, int count) {
        for (int i = 0; i < count; i++) {
            LabMessage msg = LabMessage.of(message.getKey() + "-" + i, message.getPayload() + " #" + i);
            kafkaTemplate.send(LAB_MESSAGES, msg.getKey(), msg)
                    .whenComplete((result, ex) -> {
                        if (ex == null) successCount.incrementAndGet();
                        else failureCount.incrementAndGet();
                    });
        }
        log.info("[Lab-Batch] 已提交 {} 条消息到发送队列", count);
        return count;
    }

    /**
     * ★ 事务发送 — Kafka Transactions 演示 exactly-once 语义。
     *
     * <p>两个消息在同一个事务中发送：
     * 全部成功才提交，任一失败则回滚全部。
     *
     * <p>要求：生产者配置 enable.idempotence=true，
     * transaction.id 前缀在 KafkaConfig 中已配置。
     */
    @Transactional
    public LabSendResult sendTransactional(LabMessage msg1, LabMessage msg2, boolean simulateFailure) {
        log.info("[Lab-Tx] 开始事务发送: msg1={}, msg2={}, simulateFailure={}",
                msg1.getId(), msg2.getId(), simulateFailure);

        SendResult<String, Object> r1 = null;
        try {
            r1 = kafkaTemplate.send(LAB_MESSAGES, msg1.getKey(), msg1).get(10, TimeUnit.SECONDS);
            log.info("[Lab-Tx] 第 1 条发送成功: partition={}, offset={}",
                    r1.getRecordMetadata().partition(), r1.getRecordMetadata().offset());

            if (simulateFailure) {
                throw new RuntimeException("模拟事务内失败——两条消息都将回滚");
            }

            SendResult<String, Object> r2 = kafkaTemplate.send(LAB_MESSAGES, msg2.getKey(), msg2)
                    .get(10, TimeUnit.SECONDS);
            log.info("[Lab-Tx] 第 2 条发送成功: partition={}, offset={}",
                    r2.getRecordMetadata().partition(), r2.getRecordMetadata().offset());

            successCount.addAndGet(2);
            return LabSendResult.of(msg1,
                    r1.getRecordMetadata().partition(),
                    r1.getRecordMetadata().offset(),
                    "transactional-committed");

        } catch (Exception e) {
            failureCount.addAndGet(2);
            log.error("[Lab-Tx] 事务发送失败，将回滚: {}", e.getMessage());
            throw new RuntimeException("事务发送失败", e);
        }
    }

    /**
     * 获取当前统计信息。
     */
    public LabMetrics getMetrics() {
        return new LabMetrics(successCount.get(), failureCount.get());
    }

    /**
     * 发送结果 DTO（供 Controller 返回给前端）。
     */
    public static class LabSendResult {
        private String messageId;
        private int partition;
        private long offset;
        private String mode;

        static LabSendResult of(LabMessage msg, int partition, long offset, String mode) {
            LabSendResult r = new LabSendResult();
            r.messageId = msg.getId();
            r.partition = partition;
            r.offset = offset;
            r.mode = mode;
            return r;
        }

        public String getMessageId() { return messageId; }
        public void setMessageId(String messageId) { this.messageId = messageId; }
        public int getPartition() { return partition; }
        public void setPartition(int partition) { this.partition = partition; }
        public long getOffset() { return offset; }
        public void setOffset(long offset) { this.offset = offset; }
        public String getMode() { return mode; }
        public void setMode(String mode) { this.mode = mode; }
    }

    /**
     * 指标 DTO。
     */
    public static class LabMetrics {
        private long success;
        private long failure;

        LabMetrics(long success, long failure) {
            this.success = success;
            this.failure = failure;
        }

        public long getSuccess() { return success; }
        public long getFailure() { return failure; }
    }
}
