package com.dahuaboke.redisx.from.rdb.list;

import com.dahuaboke.redisx.from.rdb.ParserManager;
import com.dahuaboke.redisx.from.rdb.base.Parser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.List;

/**
 * @Desc: zipList
 * @Author：zhh
 * @Date：2024/5/20 13:42
 */
public class ListZipListParser implements Parser {

    public List<byte[]> parse(ByteBuf byteBuf) {
        byte[] bytes = ParserManager.STRING_00.parse(byteBuf);
        // 创建一个ByteBuf
        ByteBuf buf = Unpooled.buffer();
        // 将byte数组写入ByteBuf
        buf.writeBytes(bytes);
        List<byte[]> listByte = ParserManager.ZIPLIST.parse(buf);
        // 释放ByteBuf的内存
        buf.release();
        return listByte;
    }
}
