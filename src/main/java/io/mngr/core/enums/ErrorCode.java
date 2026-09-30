package io.mngr.core.enums;

public enum ErrorCode {
    OK(0, "成功"),
    VALIDATION(1001, "参数校验失败"),
    NOT_FOUND(1002, "资源不存在"),
    DUP_NAME(2001, "名称已存在"),
    IN_USE(2002, "资源被活跃引用，禁止删除"),
    PATTERN_CONFLICT(2003, "Topic 模式冲突"),
    NOT_CONNECTED(3001, "Server 未连接"),
    CONNECT_FAILED(3002, "连接失败"),
    PUBLISH_FAILED(4001, "发布失败"),
    RATE_LIMITED(4002, "超出速率限制"),
    PACKET_TOO_LARGE(4003, "报文超过协议大小限制"),
    DECODE_FAILED(5001, "消息解析失败"),
    TEMPLATE_MISMATCH(5002, "消息与模板不匹配"),
    TEMPLATE_NOT_FOUND(6001, "模板不存在"),
    BREAKING_CHANGE(6002, "模板不兼容变更，需 force 确认"),
    TEMPLATE_IN_USE(6003, "模板被引用，禁止删除"),
    MATCH_BELOW_THRESHOLD(6004, "模板匹配度未达阈值"),
    ADMIN_DISABLED(7001, "管理操作未开启（allowAdminOps=false）"),
    CONFIRM_REQUIRED(7002, "危险操作需 confirm 回显资源名"),
    FEATURE_DISABLED(8001, "当前存储模式不支持该功能"),
    INTERNAL(9999, "内部错误");

    private final int code;
    private final String message;

    ErrorCode(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int code() { return code; }
    public String message() { return message; }
}
