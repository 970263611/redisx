package com.dahuaboke.redisx.from.rdb.module;

import io.netty.buffer.ByteBuf;

/**
 * @Desc:
 * @Author：zhh
 * @Date：2024/5/23 16:30
 */
public interface CustomModule2Parser {

    Module parseModule(ByteBuf byteBuf, int moduleVersion);
}
