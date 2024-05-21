package com.dahuaboke.redisx.slave.zhh.zset;

import com.dahuaboke.redisx.slave.zhh.ListPackParser;
import com.dahuaboke.redisx.slave.zhh.StringParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * @Desc: element与score是listPack中的element,成对出现
 *        Redis >= 7.x版本
 * @Author：zhh
 * @Date：2024/5/20 18:08
 */
public class ZSetListPackParser {
    StringParser string = new StringParser();
    ListPackParser listPack = new ListPackParser();

    public Set<ZSetEntry> parseZSetListPack(ByteBuf byteBuf){
        Set<ZSetEntry> zset = new LinkedHashSet<>();
        byte[] bytes = string.parseString(byteBuf);
        // 创建一个ByteBuf
        ByteBuf buf = Unpooled.buffer();
        // 将byte数组写入ByteBuf
        buf.writeBytes(bytes);
        List<byte[]> list = listPack.parseListPack(buf);
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
