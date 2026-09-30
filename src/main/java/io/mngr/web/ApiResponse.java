package io.mngr.web;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.UUID;

@Data
@AllArgsConstructor
public class ApiResponse<T> {
    private int code;
    private String message;
    private T data;
    private String requestId;

    public static <T> ApiResponse<T> ok(T data) {
        return new ApiResponse<>(0, "ok", data, UUID.randomUUID().toString());
    }

    public static <T> ApiResponse<T> fail(int code, String message) {
        return new ApiResponse<>(code, message, null, UUID.randomUUID().toString());
    }
}
