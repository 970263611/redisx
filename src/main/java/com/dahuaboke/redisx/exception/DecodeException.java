package com.dahuaboke.redisx.exception;

import io.netty.handler.codec.CodecException;

/**
 * 2024/5/6 16:06
 * auth: dahua
 * desc: 指令解码异常
 */
public class DecodeException extends CodecException {

    public DecodeException() {
        super();
    }

    public DecodeException(String message) {
        super(message);
    }

    public DecodeException(String message, Throwable cause) {
        super(message, cause);
    }

    public DecodeException(Throwable cause) {
        super(cause);
    }
}
