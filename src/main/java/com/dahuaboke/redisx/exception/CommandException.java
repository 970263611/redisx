package com.dahuaboke.redisx.exception;

/**
 * 2024/5/6 16:06
 * auth: dahua
 * desc: 指令相关异常
 */
public class CommandException extends Exception {

    public CommandException() {
        super();
    }

    public CommandException(String message) {
        super(message);
    }

    public CommandException(String message, Throwable cause) {
        super(message, cause);
    }

    public CommandException(Throwable cause) {
        super(cause);
    }
}
