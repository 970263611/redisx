package com.dahuaboke.redisx.slave.zhh.hash;

import com.dahuaboke.redisx.slave.zhh.StringParser;
import com.dahuaboke.redisx.slave.zhh.ZipMapParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Map;

/**
 * @Desc:
 * @Author：zhh
 * @Date：2024/5/21 9:42
 */
public class HashZipMapParser {

    StringParser string = new StringParser();
    ZipMapParser zipMap = new ZipMapParser();

    public Map<byte[],byte[]> parseHashZipMap(ByteBuf byteBuf){
        byte[] bytes = string.parseString(byteBuf);
        // 创建一个ByteBuf
        ByteBuf buf = Unpooled.buffer();
        // 将byte数组写入ByteBuf
        buf.writeBytes(bytes);
        return zipMap.parseZipMap(buf);
    }
}
