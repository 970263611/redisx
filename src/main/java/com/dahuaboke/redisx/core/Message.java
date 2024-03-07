package com.dahuaboke.redisx.core;

import io.netty.handler.codec.redis.RedisMessageType;

/**
 * author: dahua
 * date: 2024/3/5 17:08
 */
public class Message {

    private String obj;
    private RedisMessageType type;

    public Message(String obj, RedisMessageType type) {
        this.obj = obj;
        this.type = type;
    }

    public Object getObj() {
        return obj;
    }

    public RedisMessageType getType() {
        return type;
    }

    @Override
    public String toString() {
        return String.valueOf(obj);
    }
}
