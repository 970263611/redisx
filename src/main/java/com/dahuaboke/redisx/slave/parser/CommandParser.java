package com.dahuaboke.redisx.slave.parser;

import com.dahuaboke.redisx.exception.CommandException;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.List;

/**
 * 2024/5/6 11:12
 * auth: dahua
 * desc: 指令解析接口
 */
public interface CommandParser {

    List<RedisMessage> parse(ByteBuf byteBuf) throws CommandException;

    boolean matching(byte b);

    default void bind() {
        CommandParserChain.getInstance().addCommandParse(this);
    }
}
