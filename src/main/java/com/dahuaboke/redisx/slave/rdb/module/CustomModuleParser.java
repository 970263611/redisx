package com.dahuaboke.redisx.slave.rdb.module;

import io.netty.buffer.ByteBuf;

/**
 * @Desc:
 * @Author：zhh
 * @Date：2024/5/23 16:29
 */
public interface CustomModuleParser {

    Module parseModule(ByteBuf byteBuf, int moduleVersion);
}
