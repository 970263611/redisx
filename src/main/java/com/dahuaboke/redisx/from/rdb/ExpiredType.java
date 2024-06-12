package com.dahuaboke.redisx.from.rdb;

import java.io.Serializable;

/**
 * @Desc: 时间类型
 * @Author：cdl
 * @Date：2024/5/20 12:00
 */
public enum ExpiredType implements Serializable {
    /**
     * not set
     */
    NONE,
    /**
     * expired by seconds
     */
    SECOND,
    /**
     * expired by millisecond
     */
    MS
}
