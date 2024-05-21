package com.dahuaboke.redisx.slave.zhh.list;

import com.dahuaboke.redisx.slave.zhh.LengthParser;
import com.dahuaboke.redisx.slave.zhh.StringParser;
import com.dahuaboke.redisx.slave.zhh.ZipListParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.List;

/**
 * @Desc: zipList
 * @Author：zhh
 * @Date：2024/5/20 13:42
 */
public class ListZipListParser {
    StringParser string = new StringParser();
    ZipListParser zipList = new ZipListParser();

    public List<byte[]> parseListZipList(ByteBuf byteBuf){
        byte[] bytes = string.parseString(byteBuf);
        // 创建一个ByteBuf
        ByteBuf buf = Unpooled.buffer();
        // 将byte数组写入ByteBuf
        buf.writeBytes(bytes);
        List<byte[]> listByte = zipList.parseZipList(buf);
        // 释放ByteBuf的内存
        buf.release();
        return listByte;
    }
}
