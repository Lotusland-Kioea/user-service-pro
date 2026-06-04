package com.example.userservice.config;

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka 核心配置类 — 生产者、消费者、监听器容器工厂。
 *
 * <h3>这是整个 Kafka 模块中教育密度最高的文件。</h3>
 * 每个配置项均附中文注释，解释其含义、可选值和选择理由。
 *
 * <h3>架构说明</h3>
 * <ul>
 *   <li><b>ProducerFactory</b>：管理生产者连接池和默认配置</li>
 *   <li><b>ConsumerFactory</b>：管理消费者连接池和默认配置</li>
 *   <li><b>KafkaTemplate</b>：Spring 对 KafkaProducer 的高层封装，提供便捷的 send() 方法</li>
 *   <li><b>ConcurrentKafkaListenerContainerFactory</b>：驱动 @KafkaListener 注解的容器工厂</li>
 * </ul>
 *
 * <h3>生产环境调优参考</h3>
 * <table>
 *   <tr><th>场景</th><th>关键调整</th></tr>
 *   <tr><td>高吞吐量</td><td>增大 batch.size、linger.ms；增加分区数</td></tr>
 *   <tr><td>低延迟</td><td>linger.ms=0；减小 batch.size</td></tr>
 *   <tr><td>严格顺序</td><td>max.in.flight.requests=1；单分区</td></tr>
 *   <tr><td>高可用（跨 AZ）</td><td>replication-factor=3；min.insync.replicas=2</td></tr>
 *   <tr><td>Exactly-Once</td><td>启用事务 + read_committed + 幂等生产者</td></tr>
 * </table>
 *
 * @since 1.1.0
 */
@Configuration
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    // ================================================================
    // 生产者配置
    // ================================================================

    /**
     * 创建生产者工厂。
     *
     * <p><b>核心配置解释：</b>
     * <ul>
     *   <li><b>acks=all</b>：所有 ISR（In-Sync Replica）确认后才返回成功。
     *     配合 min.insync.replicas=2 使用时，至少 2 个节点确认写入。
     *     这是最高持久性保证，适合对数据丢失零容忍的场景。</li>
     *   <li><b>enable.idempotence=true</b>：开启幂等生产者。
     *     Kafka 自动为每个生产者分配 PID，broker 根据 PID + 序列号去重。
     *     保证分区内 exactly-once，配合 max.in.flight ≤ 5 使用（Kafka 3.0+）。</li>
     *   <li><b>compression.type=snappy</b>：压缩算法选择。
     *     snappy 在 CPU 开销和压缩率之间取得平衡，适合大部分场景。
     *     gzip 压缩率更高但 CPU 消耗大；zstd 是新选择。</li>
     *   <li><b>linger.ms=5</b>：消息发送前的等待时间。
     *     非 0 时，生产者等待更多消息凑成一批再发送，提高吞吐量。</li>
     *   <li><b>retries=MAX_INT + delivery.timeout.ms=120000</b>：
     *     最多重试 120 秒，配合幂等生产者保证不重复。</li>
     * </ul>
     */
    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> props = new HashMap<>();

        // ———— 连接 ————
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092,localhost:9094,localhost:9096");

        // ———— 序列化（注册 JavaTimeModule 处理 LocalDateTime） ————
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        // ★ 用自定义 JsonSerializer（内部 ObjectMapper 注册了 JavaTimeModule）
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                JsonSerializer.class);

        // ———— 事务 ————
        // ★ 事务 ID 前缀：启用 Kafka 事务（配合 @Transactional 使用）
        // 生产环境中每个实例使用不同的前缀（如 hostname），避免冲突
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                "user-service-pro-tx-");

        // ———— 可靠性 ————
        // acks=all (= -1)：所有 ISR 确认后才返回成功，最高持久性
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // 幂等生产者：broker 端自动去重，保证分区内 exactly-once
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        // 最大重试次数：配合 delivery.timeout.ms 使用
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        // 发送超时总窗口（120 秒）
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);

        // ———— 性能 ————
        // 压缩算法：snappy 在 CPU 和压缩率间平衡
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        // 等待 5ms 凑批，提升批量效率
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        // 单批次最大字节数 16KB
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // 单连接最大并发未确认请求数（幂等生产者最多 5）
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        // 单次请求超时（30 秒）
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);

        // ———— 缓冲区 ————
        // 生产者总缓冲区大小 32MB
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        log.info("Kafka ProducerFactory 配置完成：acks=all, idempotence=true, compression=snappy, tx-prefix=user-service-pro-tx-");

        DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(props);
        // ★ 注册 JavaTimeModule，确保 LocalDateTime 正确序列化为 ISO 字符串
        // （JsonSerializer 默认的 ObjectMapper 不含 JSR310 模块）
        factory.setValueSerializer(new JsonSerializer<>(
                new com.fasterxml.jackson.databind.ObjectMapper()
                        .registerModule(new JavaTimeModule())
        ));
        return factory;
    }

    /**
     * 创建 KafkaTemplate — Spring 对 KafkaProducer 的高级封装。
     *
     * <p>提供便捷方法：
     * <ul>
     *   <li>{@code send(topic, value)} — 异步发送，返回 {@code CompletableFuture}</li>
     *   <li>{@code send(topic, key, value)} — 指定 Key 发送（Key 决定分区）</li>
     *   <li>{@code send(topic, partition, key, value)} — 指定分区发送</li>
     *   <li>{@code sendDefault(value)} — 发送到默认 Topic</li>
     *   <li>{@code executeInTransaction(ops)} — 事务中执行</li>
     * </ul>
     */
    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(producerFactory());
        // 设置默认 Topic（调用 sendDefault 时使用）
        template.setDefaultTopic("lab-messages");
        return template;
    }

    // ================================================================
    // 消费者配置
    // ================================================================

    /**
     * 创建消费者工厂。
     *
     * <p><b>核心配置解释：</b>
     * <ul>
     *   <li><b>enable.auto.commit=false</b>：禁用自动提交偏移量。
     *     改为手动提交（{@code Acknowledgment.acknowledge()}），
     *     消费者处理完消息后才提交，避免"已提交但未处理"的数据丢失。</li>
     *   <li><b>isolation.level=read_committed</b>：事务读隔离。
     *     只读取已提交的事务消息，配合生产者事务实现端到端 exactly-once。</li>
     *   <li><b>auto.offset.reset=earliest</b>：首次消费或 offset 丢失时从最早开始。
     *     latest 反之（只消费新消息）。</li>
     * </ul>
     */
    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();

        // ———— 连接 ————
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092,localhost:9094,localhost:9096");

        // ———— 反序列化 ————
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                JsonDeserializer.class);

        // ———— 消费者组 ————
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "user-service-group");
        // 首次消费从最早开始（学习项目需要看到历史消息）
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // ———— 手动提交 ————
        // 禁用自动提交，由消费者显式调用 ack()
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // ———— 事务读隔离 ————
        // 只读取已提交的事务消息，配合生产者事务
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        // ———— 拉取 ————
        // 单次 poll 最多拉取 500 条
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);

        // ———— 心跳 ————
        // 会话超时 45 秒
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 45_000);
        // 心跳间隔 15 秒（会话超时的 1/3）
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 15_000);

        // ———— JSON 反序列化安全 ————
        // 仅信任指定包下的类，防止反序列化漏洞攻击
        props.put(JsonDeserializer.TRUSTED_PACKAGES,
                "com.example.userservice.event,com.example.userservice.lab");

        log.info("Kafka ConsumerFactory 配置完成：manual-commit, isolation=read_committed, group=user-service-group");

        DefaultKafkaConsumerFactory<String, Object> factory = new DefaultKafkaConsumerFactory<>(props);
        // ★ 注册 JavaTimeModule，确保 LocalDateTime 正确反序列化
        factory.setValueDeserializer(new JsonDeserializer<>(
                new com.fasterxml.jackson.databind.ObjectMapper()
                        .registerModule(new JavaTimeModule())
        ));
        return factory;
    }

    /**
     * 标准监听器容器工厂 — 单条消息处理 + 手动确认。
     *
     * <p>用于 {@link com.example.userservice.messaging.UserEventConsumer}
     * 和 {@link com.example.userservice.lab.KafkaLabConsumer}。
     *
     * <p>并发数 3 对应 3 个分区，每个消费者线程独占一个分区并行处理。
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object>
            kafkaListenerContainerFactory(
                    ConsumerFactory<String, Object> consumerFactory,
                    com.example.userservice.messaging.KafkaErrorHandler errorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        // ★ 并发数：3 个线程，每个线程消费一个分区
        factory.setConcurrency(3);

        // ★ AckMode.MANUAL：消费者必须显式调用 Acknowledgment.acknowledge()
        // 这是解决 "invokeHandler Failed" 的关键 —— @KafkaListener
        // 的方法签名中有 Acknowledgment 参数时，必须设置 MANUAL 模式
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        // ★ 设置统一错误处理器（重试 + DLT）
        factory.setCommonErrorHandler(errorHandler.createErrorHandler());

        log.info("KafkaListenerContainerFactory 配置完成：concurrency=3, ack-mode=MANUAL");

        return factory;
    }

    /**
     * 批量消费容器工厂 — 一次处理多条消息。
     *
     * <p>用于 {@link com.example.userservice.lab.KafkaLabConsumer} 的批量消费模式。
     * 通过 {@code @KafkaListener(containerFactory = "batchListenerContainerFactory")} 指定使用。
     *
     * <p>批量消费的优势：减少网络往返，提高吞吐量。
     * 但需要注意处理失败时整批消息的补偿策略。
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object>
            batchListenerContainerFactory(
                    ConsumerFactory<String, Object> consumerFactory,
                    com.example.userservice.messaging.KafkaErrorHandler errorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        // ★ 开启批量消费模式：@KafkaListener 接收 List<ConsumerRecord>
        factory.setBatchListener(true);
        factory.setConcurrency(3);
        // ★ 批量消费也需显式设置 AckMode
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setCommonErrorHandler(errorHandler.createErrorHandler());

        log.info("BatchListenerContainerFactory 配置完成：batch=true, concurrency=3, ack-mode=MANUAL");

        return factory;
    }
}
