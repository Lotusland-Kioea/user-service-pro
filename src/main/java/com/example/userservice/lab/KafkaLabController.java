package com.example.userservice.lab;

import com.example.userservice.common.ApiResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka 学习实验室 — REST 控制器。
 *
 * <h3>设计理念</h3>
 * <p>每个端点独立演示一个 Kafka 概念，所有响应包含详细的
 * partition/offset/mode 等元数据，无需查看日志即可理解行为。
 *
 * <h3>端点一览</h3>
 * <table>
 *   <tr><th>端点</th><th>演示概念</th></tr>
 *   <tr><td>POST /kafka-lab/send</td><td>异步发送 + 回调</td></tr>
 *   <tr><td>POST /kafka-lab/send-sync</td><td>同步发送 + 超时</td></tr>
 *   <tr><td>POST /kafka-lab/send-with-key/{key}</td><td>分区路由（相同 Key → 同一分区）</td></tr>
 *   <tr><td>POST /kafka-lab/send-batch/{count}</td><td>批量发送</td></tr>
 *   <tr><td>POST /kafka-lab/send-transactional</td><td>事务（exactly-once）</td></tr>
 *   <tr><td>POST /kafka-lab/send-error-demo</td><td>触发消费失败 → 重试 → DLT</td></tr>
 *   <tr><td>GET /kafka-lab/topics</td><td>集群元数据</td></tr>
 *   <tr><td>GET /kafka-lab/metrics</td><td>生产者统计</td></tr>
 * </table>
 *
 * @since 1.1.0
 */
@RestController
@RequestMapping("/kafka-lab")
public class KafkaLabController {

    private static final Logger log = LoggerFactory.getLogger(KafkaLabController.class);

    private final KafkaLabProducer producer;
    private final KafkaLabConsumer consumer;

    public KafkaLabController(KafkaLabProducer producer, KafkaLabConsumer consumer) {
        this.producer = producer;
        this.consumer = consumer;
    }

    // ================================================================
    // 1. 异步发送
    // ================================================================

    /**
     * ★ 异步发送 — 最常用的生产模式。
     *
     * <p>消息入队后立即返回，不等待 broker 确认。
     * 实际 partition/offset 在回调中输出到日志。
     *
     * <p>请求体示例：
     * <pre>{@code {"payload": "hello kafka", "key": "test"}}</pre>
     */
    @PostMapping("/send")
    public ApiResponse<KafkaLabProducer.LabSendResult> sendAsync(@RequestBody LabMessage message) {
        if (message.getKey() == null || message.getPayload() == null) {
            return ApiResponse.error(400, "key 和 payload 不能为空");
        }
        KafkaLabProducer.LabSendResult result = producer.sendAsync(message);
        return ApiResponse.success(result);
    }

    // ================================================================
    // 2. 同步发送
    // ================================================================

    /**
     * ★ 同步发送 — 阻塞等待确认，响应中直接返回 partition 和 offset。
     *
     * <p>适合需要立即知道发送结果的场景。
     */
    @PostMapping("/send-sync")
    public ApiResponse<KafkaLabProducer.LabSendResult> sendSync(@RequestBody LabMessage message) {
        if (message.getKey() == null || message.getPayload() == null) {
            return ApiResponse.error(400, "key 和 payload 不能为空");
        }
        KafkaLabProducer.LabSendResult result = producer.sendSync(message, 10);
        return ApiResponse.success(result);
    }

    // ================================================================
    // 3. 指定 Key 发送 — 演示分区亲和性
    // ================================================================

    /**
     * ★ 指定 Key 发送 — 演示相同 Key 路由到同一分区。
     *
     * <p>连续发送 5 条相同 Key 的消息，观察全部落在同一分区。
     * 在 Kafdrop 中验证：所有消息的 partition 号相同。
     *
     * <p>使用方法：
     * <pre>POST /kafka-lab/send-with-key/user-42</pre>
     */
    @PostMapping("/send-with-key/{key}")
    public ApiResponse<Map<String, Object>> sendWithKey(@PathVariable String key) {
        Map<String, Object> data = new HashMap<>();
        java.util.List<KafkaLabProducer.LabSendResult> results = new java.util.ArrayList<>();

        for (int i = 1; i <= 5; i++) {
            LabMessage msg = LabMessage.of(key, "消息 #" + i + " (Key=" + key + ")");
            KafkaLabProducer.LabSendResult result = producer.sendSync(msg, 10);
            results.add(result);
            log.info("[Key演示] #{}: partition={}", i, result.getPartition());
        }

        data.put("key", key);
        data.put("count", 5);
        data.put("results", results);
        data.put("note", "观察所有消息是否在同一分区");

        return ApiResponse.success(data);
    }

    // ================================================================
    // 4. 批量发送
    // ================================================================

    /**
     * ★ 批量发送 — 连续发送 N 条消息，观察 batching 效果。
     *
     * <p>使用方法：
     * <pre>POST /kafka-lab/send-batch/20</pre>
     */
    @PostMapping("/send-batch/{count}")
    public ApiResponse<Map<String, Object>> sendBatch(@PathVariable int count) {
        if (count > 1000) {
            return ApiResponse.error(400, "单次最多 1000 条");
        }
        LabMessage msg = LabMessage.of("batch-test", "批量消息");
        long sent = producer.sendBatch(msg, count);

        Map<String, Object> data = new HashMap<>();
        data.put("submitted", sent);
        data.put("note", "消息已提交到发送队列，观察 linger.ms=5 的批量效果");

        return ApiResponse.success(data);
    }

    // ================================================================
    // 5. 事务发送
    // ================================================================

    /**
     * ★ 事务发送 — 演示 Kafka Transactions 的 exactly-once 语义。
     *
     * <p>在一个事务中发送 2 条消息：
     * <ul>
     *   <li>{@code simulateFailure=false}：两条都成功提交</li>
     *   <li>{@code simulateFailure=true}：两条都回滚，消费者看不到任何一条</li>
     * </ul>
     */
    @PostMapping("/send-transactional")
    public ApiResponse<KafkaLabProducer.LabSendResult> sendTransactional(
            @RequestParam(defaultValue = "false") boolean simulateFailure) {

        LabMessage msg1 = LabMessage.of("tx-test", "事务消息 1");
        LabMessage msg2 = LabMessage.of("tx-test", "事务消息 2");

        try {
            KafkaLabProducer.LabSendResult result = producer.sendTransactional(msg1, msg2, simulateFailure);
            return ApiResponse.success(result);
        } catch (Exception e) {
            return ApiResponse.error(500, "事务发送失败: " + e.getMessage());
        }
    }

    // ================================================================
    // 6. 错误演示 — DLT 流程
    // ================================================================

    /**
     * ★ 错误演示 — 发送一条消息，消费者必定失败。
     *
     * <p>配合 {@code kafka.demo.fail-chance=1.0} 时，消费者每次处理都抛异常：
     * <ol>
     *   <li>第 1 次消费 → 失败 → 2s 后重试</li>
     *   <li>第 2 次消费 → 失败 → 2s 后重试</li>
     *   <li>第 3 次消费 → 失败 → 2s 后重试（共 3 次重试）</li>
     *   <li>重试耗尽 → 消息进入 lab-messages-dlt</li>
     * </ol>
     *
     * <p>在 Kafdrop 中查看 {@code lab-messages-dlt} 确认消息已转入死信队列。
     */
    @PostMapping("/send-error-demo")
    public ApiResponse<Map<String, Object>> sendErrorDemo() {
        LabMessage msg = LabMessage.of("error-test", "这条消息消费者会处理失败");

        KafkaLabProducer.LabSendResult result = producer.sendSync(msg, 10);

        Map<String, Object> data = new HashMap<>();
        data.put("message", result);
        data.put("note", "设置 kafka.demo.fail-chance=1.0 后观察 DLT 流程:");
        data.put("steps", new String[]{
                "1. 消费者处理 → 失败 → 2s 后重试",
                "2. 消费者处理 → 失败 → 2s 后重试",
                "3. 消费者处理 → 失败 → 2s 后重试（共 3 次固定间隔）",
                "4. 3 次重试耗尽 → 消息进入 lab-messages-dlt"
        });

        return ApiResponse.success(data);
    }

    // ================================================================
    // 7. Topic 信息
    // ================================================================

    /**
     * ★ 集群元数据 — 返回所有应用相关 Topic 的说明。
     *
     * <p>生产环境可用 {@code KafkaAdmin.describeTopics()} 动态获取。
     */
    @GetMapping("/topics")
    public ApiResponse<Map<String, Object>> getTopics() {
        Map<String, Object> data = new HashMap<>();

        data.put("user-events", Map.of(
                "partitions", 3,
                "purpose", "用户事件（创建/更新/删除）",
                "producer", "UserEventPublisher",
                "consumer", "UserEventConsumer (group: user-service-group)"
        ));

        data.put("lab-messages", Map.of(
                "partitions", 4,
                "purpose", "实验室消息",
                "producer", "KafkaLabProducer",
                "consumer", "KafkaLabConsumer (groups: lab-consumer-group, lab-consumer-group-batch)"
        ));

        data.put("consumerCount", consumer.getConsumedCount());

        return ApiResponse.success(data);
    }

    // ================================================================
    // 8. 生产者指标
    // ================================================================

    /**
     * ★ 生产者统计 — 成功/失败计数。
     */
    @GetMapping("/metrics")
    public ApiResponse<KafkaLabProducer.LabMetrics> getMetrics() {
        return ApiResponse.success(producer.getMetrics());
    }
}
