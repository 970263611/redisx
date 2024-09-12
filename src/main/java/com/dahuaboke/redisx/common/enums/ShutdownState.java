package com.dahuaboke.redisx.common.enums;

/**
 * author: dahua
 * date: 2024/9/12 17:13
 */
public enum ShutdownState {

    BEGINNING,
    WAIT_WRITE_OFFSET,
    WAIT_COMMIT_OFFSET,
    ENDED;
}
