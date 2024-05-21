package com.dahuaboke.redisx.slave.rdb.base;

import io.netty.buffer.ByteBuf;

/**
 * 2024/5/17 10:00
 * auth: dahua
 * desc:
 */
public interface Parser<T> {

    T parse(ByteBuf byteBuf) throws Exception;
}
