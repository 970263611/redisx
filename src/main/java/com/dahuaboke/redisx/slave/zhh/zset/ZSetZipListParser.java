package com.dahuaboke.redisx.slave.zhh.zset;

import com.dahuaboke.redisx.slave.zhh.StringParser;
import com.dahuaboke.redisx.slave.zhh.ZipListParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * @Desc: element与score是zipList中的element,成对出现
 * @Author：zhh
 * @Date：2024/5/20 17:43
 */
public class ZSetZipListParser {
    StringParser string = new StringParser();
    ZipListParser zipList = new ZipListParser();

    public Set<ZSetEntry> parseZSetZipList(ByteBuf byteBuf){
        Set<ZSetEntry> zset = new LinkedHashSet<>();
        byte[] bytes = string.parseString(byteBuf);
        // 创建一个ByteBuf
        ByteBuf buf = Unpooled.buffer();
        // 将byte数组写入ByteBuf
        buf.writeBytes(bytes);
        List<byte[]> list = zipList.parseZipList(buf);
        for (int i = 0; i < list.size(); i += 2) {
            // 检查是否还有足够的元素来形成一对
            if (i + 1 < list.size()) {
                byte[] element = list.get(i);
                byte[] score = list.get(i + 1);
                zset.add(new ZSetEntry(element, Double.valueOf(new String(score, StandardCharsets.UTF_8))));
            } else {
                break;
            }
        }
        return zset;
    }
}
