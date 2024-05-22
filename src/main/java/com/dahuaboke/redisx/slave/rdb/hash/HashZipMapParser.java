package com.dahuaboke.redisx.slave.rdb.hash;

import com.dahuaboke.redisx.slave.rdb.ParserManager;
import com.dahuaboke.redisx.slave.rdb.base.Parser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Map;

/**
 * @Desc:
 * @Author：zhh
 * @Date：2024/5/21 9:42
 */
public class HashZipMapParser implements Parser {

    public Map<byte[], byte[]> parse(ByteBuf byteBuf) {
        byte[] bytes = ParserManager.STRING_00.parse(byteBuf);
        // 创建一个ByteBuf
        ByteBuf buf = Unpooled.buffer();
        // 将byte数组写入ByteBuf
        buf.writeBytes(bytes);
        return ParserManager.ZIPMAP.parse(buf);
    }
}
