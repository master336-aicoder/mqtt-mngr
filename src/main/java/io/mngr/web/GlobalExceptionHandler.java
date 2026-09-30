package io.mngr.web;

import io.mngr.core.enums.ErrorCode;
import io.mngr.core.exception.MngrException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.stream.Collectors;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(MngrException.class)
    public ResponseEntity<ApiResponse<Object>> mngr(MngrException e) {
        HttpStatus status = switch (e.getCode()) {
            case 1002 -> HttpStatus.NOT_FOUND;
            case 8001 -> HttpStatus.SERVICE_UNAVAILABLE;
            case 7001, 7002 -> HttpStatus.FORBIDDEN;
            case 3001, 3002 -> HttpStatus.SERVICE_UNAVAILABLE;
            case 401 -> HttpStatus.UNAUTHORIZED;
            case 403 -> HttpStatus.FORBIDDEN;
            default -> HttpStatus.BAD_REQUEST;
        };
        if (e.getCode() == 9999) log.error("internal error", e);
        ApiResponse<Object> r = ApiResponse.fail(e.getCode(), e.getMessage());
        r.setData(e.getDetail() == null ? null : java.util.Map.of("detail", e.getDetail()));
        return ResponseEntity.status(status).body(r);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiResponse<Object>> valid(MethodArgumentNotValidException e) {
        String msg = e.getBindingResult().getFieldErrors().stream()
                .map(f -> f.getField() + ": " + f.getDefaultMessage())
                .collect(Collectors.joining("; "));
        return ResponseEntity.badRequest().body(ApiResponse.fail(ErrorCode.VALIDATION.code(), msg));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ApiResponse<Object>> illegal(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(ApiResponse.fail(ErrorCode.VALIDATION.code(), e.getMessage()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiResponse<Object>> other(Exception e) {
        String tid = java.util.UUID.randomUUID().toString();
        log.error("unhandled (traceId={})", tid, e);
        return ResponseEntity.status(500).body(ApiResponse.fail(
                ErrorCode.INTERNAL.code(), ErrorCode.INTERNAL.message() + " traceId=" + tid));
    }
}
