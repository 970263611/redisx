package com.dahuaboke.redisx.slave.rdb.zset;

import com.dahuaboke.redisx.slave.rdb.ParserManager;
import com.dahuaboke.redisx.slave.rdb.base.Parser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * @Desc: element与score是listPack中的element, 成对出现
 * Redis >= 7.x版本
 * @Author：zhh
 * @Date：2024/5/20 18:08
 */
public class ZSetListPackParser implements Parser {

    public Set<ZSetEntry> parse(ByteBuf byteBuf) {
        Set<ZSetEntry> zset = new LinkedHashSet<>();
        byte[] bytes = ParserManager.STRING_00.parse(byteBuf);
        // 创建一个ByteBuf
        ByteBuf buf = Unpooled.buffer();
        // 将byte数组写入ByteBuf
        buf.writeBytes(bytes);
        List<byte[]> list = ParserManager.LISTPACK.parse(buf);
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
