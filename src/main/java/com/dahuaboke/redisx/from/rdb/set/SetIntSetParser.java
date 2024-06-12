package com.dahuaboke.redisx.from.rdb.set;

import com.dahuaboke.redisx.from.rdb.ParserManager;
import com.dahuaboke.redisx.from.rdb.base.Parser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Set;

/**
 * @Desc: Set存储的全是数字时触发SetIntSet
 * @Author：zhh
 * @Date：2024/5/20 15:54
 */
public class SetIntSetParser implements Parser {

    public Set<byte[]> parse(ByteBuf byteBuf) {
        byte[] bytes = ParserManager.STRING_00.parse(byteBuf);
        // 创建一个ByteBuf
        ByteBuf buf = Unpooled.buffer();
        // 将byte数组写入ByteBuf
        buf.writeBytes(bytes);
        Set<byte[]> setByte = ParserManager.INTSET.parse(buf);
        // 释放ByteBuf的内存
        buf.release();
        return setByte;
    }
}
