package com.dahuaboke.redisx.slave.rdb.list;

import com.dahuaboke.redisx.slave.rdb.base.LengthParser;
import com.dahuaboke.redisx.slave.rdb.base.Parser;
import com.dahuaboke.redisx.slave.rdb.base.StringParser;
import com.dahuaboke.redisx.slave.rdb.base.ZipListParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.LinkedList;
import java.util.List;

/**
 * @Desc: quickList = list+zipList
 *        Redis < 7.x 版本
 * @Author：zhh
 * @Date：2024/5/20 13:48
 */
public class ListQuickListParser implements Parser {
    LengthParser length = new LengthParser();
    StringParser string = new StringParser();
    ZipListParser zipList = new ZipListParser();

    public List<byte[]> parse(ByteBuf byteBuf){
        //元素个数
        long len = length.parse(byteBuf).len;
        List<byte[]> list = new LinkedList<>();
        for (int i = 0; i < len; i++) {
            byte[] bytes = string.parse(byteBuf);
            // 创建一个ByteBuf
            ByteBuf buf = Unpooled.buffer();
            // 将byte数组写入ByteBuf
            buf.writeBytes(bytes);
            List<byte[]> listByte = zipList.parse(buf);
            list.addAll(listByte);
            // 释放ByteBuf的内存
            buf.release();
        }
        return list;
    }
}
