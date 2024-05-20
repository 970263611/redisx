package com.dahuaboke.redisx.slave.zhh.list;

import com.dahuaboke.redisx.slave.zhh.LengthParser;
import com.dahuaboke.redisx.slave.zhh.LengthParser.Len;
import com.dahuaboke.redisx.slave.zhh.StringParser;
import com.dahuaboke.redisx.slave.zhh.ZipListParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.LinkedList;
import java.util.List;

/**
 * @Desc: quickList = list+zipList  Redis 6.x版本
 * @Author：zhh
 * @Date：2024/5/20 13:48
 */
public class ListQuickListParser {
    LengthParser length = new LengthParser();
    StringParser string = new StringParser();
    ZipListParser zipList = new ZipListParser();

    public List<byte[]> parseQuickList(ByteBuf byteBuf){
        //元素个数
        long len = length.parseLength(byteBuf).len;
        List<byte[]> list = new LinkedList<>();
        for (int i = 0; i < len; i++) {
            byte[] bytes = string.parseString(byteBuf);
            // 创建一个ByteBuf
            ByteBuf buf = Unpooled.buffer();
            // 将byte数组写入ByteBuf
            buf.writeBytes(bytes);
            List<byte[]> listByte = zipList.parseZipList(buf);
            list.addAll(listByte);
            // 释放ByteBuf的内存
            buf.release();
        }
        return list;
    }
}
