package io.mngr.core.exception;

import io.mngr.core.enums.ErrorCode;

public class MngrException extends RuntimeException {
    private final int code;
    private final transient Object detail;

    public MngrException(ErrorCode ec) {
        this(ec, ec.message(), null);
    }

    public MngrException(ErrorCode ec, String message) {
        this(ec, message, null);
    }

    public MngrException(ErrorCode ec, String message, Throwable cause) {
        super(message, cause);
        this.code = ec.code();
        this.detail = null;
    }

    public MngrException(ErrorCode ec, String message, Object detail) {
        super(message);
        this.code = ec.code();
        this.detail = detail;
    }

    public int getCode() { return code; }
    public Object getDetail() { return detail; }

    public static MngrException of(ErrorCode ec, String fmt, Object... args) {
        return new MngrException(ec, String.format(fmt, args));
    }
}
