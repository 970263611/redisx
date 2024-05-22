package com.dahuaboke.redisx.slave.rdb.set;

import com.dahuaboke.redisx.slave.rdb.base.IntSetParser;
import com.dahuaboke.redisx.slave.rdb.base.Parser;
import com.dahuaboke.redisx.slave.rdb.base.StringParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Set;

/**
 * @Desc: Set存储的全是数字时触发SetIntSet
 * @Author：zhh
 * @Date：2024/5/20 15:54
 */
public class SetIntSetParser implements Parser {
    StringParser string = new StringParser();
    IntSetParser intSet = new IntSetParser();

    public Set<byte[]> parse(ByteBuf byteBuf){
        byte[] bytes = string.parse(byteBuf);
        // 创建一个ByteBuf
        ByteBuf buf = Unpooled.buffer();
        // 将byte数组写入ByteBuf
        buf.writeBytes(bytes);
        Set<byte[]> setByte = intSet.parse(buf);
        // 释放ByteBuf的内存
        buf.release();
        return setByte;
    }
}
