package com.dahuaboke.redisx.slave.rdb.set;

import com.dahuaboke.redisx.slave.rdb.ParserManager;
import com.dahuaboke.redisx.slave.rdb.base.Parser;
import io.netty.buffer.ByteBuf;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @Desc: Set存储的非数字时触发Set
 * Redis < 7.x 版本
 * @Author：zhh
 * @Date：2024/5/20 15:47
 */
public class SetParser implements Parser {

    public Set<byte[]> parse(ByteBuf byteBuf) {
        long len = ParserManager.LENGTH.parse(byteBuf).len;
        Set<byte[]> set = new LinkedHashSet<>();
        for (int i = 0; i < len; i++) {
            byte[] element = ParserManager.STRING_00.parse(byteBuf);
            set.add(element);
        }
        return set;
    }
}
