package io.mngr.core.exception;

import io.mngr.core.enums.ErrorCode;
import io.mngr.core.enums.FailKind;
import lombok.Getter;

/** Adapter 层发布失败的协议无关分类（§7.4）。 */
@Getter
public class PublishException extends RuntimeException {
    private final FailKind kind;
    private final ErrorCode errorCode;

    public PublishException(FailKind kind, ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.kind = kind;
        this.errorCode = errorCode;
    }

    public static PublishException of(FailKind kind, ErrorCode ec, String msg) {
        return new PublishException(kind, ec, msg, null);
    }
}
