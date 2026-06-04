package com.example.userservice.event;

/**
 * 用户领域事件类型枚举。
 *
 * <h3>设计说明</h3>
 * 事件驱动架构中，每个业务操作对应一个事件类型。
 * Consumer 根据事件类型做不同的后续处理：
 * <ul>
 *   <li>{@code CREATED} — 发送欢迎邮件、初始化用户偏好设置等</li>
 *   <li>{@code UPDATED} — 同步用户信息到其他系统、审计日志等</li>
 *   <li>{@code DELETED} — 清理关联数据、通知下游服务等</li>
 * </ul>
 *
 * @since 1.1.0
 */
public enum UserEventType {
    CREATED,
    UPDATED,
    DELETED
}
