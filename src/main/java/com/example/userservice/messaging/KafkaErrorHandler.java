package com.example.userservice.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Kafka 消费端统一错误处理器 — 生产级重试 + 死信队列（DLT）。
 *
 * <h3>核心机制</h3>
 * <ol>
 *   <li><b>重试</b>：消费失败后自动重试 3 次，退避间隔固定 2s（FixedBackOff）</li>
 *   <li><b>死信队列</b>：重试耗尽后将消息转发到 {@code {原topic}-dlt}，
 *       并在 Header 中保留原始元数据（topic、partition、offset、异常信息），便于排查</li>
 *   <li><b>错误分类</b>：
 *     <ul>
 *       <li>反序列化异常（{@code DeserializationException}）不重试，直接进 DLT（毒丸消息）</li>
 *       <li>其他异常（如业务异常、超时）可重试</li>
 *     </ul>
 *   </li>
 * </ol>
 *
 * <h3>死信消息 Header（自动添加）</h3>
 * <ul>
 *   <li>{@code kafka_dlt-original-topic}：原 Topic 名称</li>
 *   <li>{@code kafka_dlt-original-partition}：原分区号</li>
 *   <li>{@code kafka_dlt-original-offset}：原偏移量</li>
 *   <li>{@code kafka_dlt-exception-message}：异常消息</li>
 *   <li>{@code kafka_dlt-exception-stacktrace}：异常堆栈</li>
 * </ul>
 *
 * <h3>学习要点</h3>
 * <ul>
 *   <li>反序列化异常不重试：畸形消息（毒丸）重试多少次都会失败，直接隔离</li>
 *   <li>固定退避 vs 指数退避：
 *     固定退避（FixedBackOff）简单可控，适合学习；
 *     指数退避（ExponentialBackOff）更适合生产环境的瞬时故障</li>
 *   <li>DLT 监控告警：生产环境中应监控 DLT 的消息量，
 *     超过阈值触发告警，安排人工处理</li>
 * </ul>
 *
 * @since 1.1.0
 */
@Component
public class KafkaErrorHandler {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandler.class);

    /**
     * 最大重试次数。
     */
    private static final long MAX_RETRIES = 3L;

    /**
     * 初始退避间隔（毫秒）— 2 秒。
     * FixedBackOff 的间隔不递增，每次都是固定值。
     */
    private static final long BACKOFF_INTERVAL = 2000L;

    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * 通过构造注入 KafkaTemplate，供 DeadLetterPublishingRecoverer 使用。
     * <p>
     * Spring Kafka 3.x 要求 DeadLetterPublishingRecoverer 必须持有
     * KafkaOperations（即 KafkaTemplate）来发送死信消息。
     */
    public KafkaErrorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * 创建 {@link DefaultErrorHandler}。
     * <p>
     * 注：此方法由 {@code KafkaConfig} 调用，用于配置
     * {@code ConcurrentKafkaListenerContainerFactory}。
     *
     * @return 配置完成的 DefaultErrorHandler
     */
    public DefaultErrorHandler createErrorHandler() {
        // DeadLetterPublishingRecoverer：将失败消息转发到 DLT
        // 第一个参数是 KafkaTemplate，第二个参数是 DestinationResolver：
        // DLT 命名规则：{原 Topic}-dlt
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                // ★ DLT 命名规则：{原 Topic}-dlt，分区 -1 表示由 Kafka 自动选择
                // DLT topic 只有 1 个分区，不能按原始 partition 投递（否则分区 1/2 会失败）
                (cr, e) -> new TopicPartition(
                        cr.topic() + "-dlt",
                        -1  // 由 Kafka 决定分区（DLT topic 只有 1 个分区，始终落入分区 0）
                )
        );

        // 固定间隔退避：2s 间隔，最多重试 3 次
        // 生产环境可考虑 ExponentialBackOff 或 ExponentialRandomBackOff
        FixedBackOff backOff = new FixedBackOff(BACKOFF_INTERVAL, MAX_RETRIES);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

        // ★ 反序列化异常不重试 — 毒丸消息直接进 DLT
        // 原因：反序列化失败的消息无论重试多少次都会失败，直接隔离更高效
        errorHandler.addNotRetryableExceptions(
                org.springframework.kafka.support.serializer.DeserializationException.class
        );

        // ★ 添加自定义的恢复回调（记录详细日志）
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            if (deliveryAttempt > 0) {
                log.warn("Kafka 消费重试中：第 {} 次, topic={}, partition={}, offset={}, error={}",
                        deliveryAttempt,
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        ex.getMessage());
            }
        });

        // ★ DLT 转发前后的日志记录
        // 注意：setSeekAfterError 对于可重试异常会重置 offset 以重新消费；
        // 对于不可重试异常（如反序列化失败）不建议 seek，因为 DLT 已经处理了
        errorHandler.setSeekAfterError(false);

        return errorHandler;
    }
}
