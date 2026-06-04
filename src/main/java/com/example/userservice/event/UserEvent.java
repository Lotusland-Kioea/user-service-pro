package com.example.userservice.event;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * 用户领域事件。
 *
 * <h3>字段说明</h3>
 * <ul>
 *   <li><b>eventId</b>：事件唯一标识（UUID），用于 Consumer 端幂等去重。
 *     Consumer 可基于 eventId 判断是否已处理过此事件</li>
 *   <li><b>eventType</b>：事件类型（CREATED / UPDATED / DELETED）</li>
 *   <li><b>userId、userName、userEmail</b>：事件携带的用户数据快照</li>
 *   <li><b>eventTimestamp</b>：事件产生时间，用于延迟监控和排序</li>
 *   <li><b>source</b>：事件来源服务标识，多服务场景下区分事件来源</li>
 * </ul>
 *
 * <h3>为什么用不可变 POJO 而非 Java Record？</h3>
 * Jackson 对 Record 的序列化/反序列化在 Spring Kafka 3.2 以下版本
 * 存在兼容性问题（需要 @JsonCreator 等额外注解）。POJO 更稳定可靠。
 *
 * @since 1.1.0
 */
public class UserEvent {

    /** 事件唯一标识（UUID 字符串），用于 Consumer 幂等去重 */
    private String eventId;

    /** 事件类型 */
    private UserEventType eventType;

    /** 用户 ID */
    private Long userId;

    /** 用户名（事件发生时的快照） */
    private String userName;

    /** 用户邮箱（事件发生时的快照） */
    private String userEmail;

    /** 事件产生时间 */
    private LocalDateTime eventTimestamp;

    /** 事件来源（固定 "user-service"） */
    private String source;

    /**
     * 无参构造（Jackson 反序列化要求）。
     */
    public UserEvent() {
    }

    /**
     * 创建用户事件。
     *
     * @param eventType 事件类型
     * @param userId    用户 ID
     * @param userName  用户名
     * @param userEmail 用户邮箱
     * @return 构建完成的事件对象
     */
    public static UserEvent of(UserEventType eventType, Long userId, String userName, String userEmail) {
        UserEvent event = new UserEvent();
        event.eventId = UUID.randomUUID().toString();
        event.eventType = eventType;
        event.userId = userId;
        event.userName = userName;
        event.userEmail = userEmail;
        event.eventTimestamp = LocalDateTime.now();
        event.source = "user-service";
        return event;
    }

    // ======== Getters and Setters ========

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public UserEventType getEventType() {
        return eventType;
    }

    public void setEventType(UserEventType eventType) {
        this.eventType = eventType;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getUserEmail() {
        return userEmail;
    }

    public void setUserEmail(String userEmail) {
        this.userEmail = userEmail;
    }

    public LocalDateTime getEventTimestamp() {
        return eventTimestamp;
    }

    public void setEventTimestamp(LocalDateTime eventTimestamp) {
        this.eventTimestamp = eventTimestamp;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    @Override
    public String toString() {
        return "UserEvent{" +
                "eventId='" + eventId + '\'' +
                ", eventType=" + eventType +
                ", userId=" + userId +
                ", userName='" + userName + '\'' +
                ", source='" + source + '\'' +
                '}';
    }
}
