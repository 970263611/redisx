package com.dahuaboke.redisx.from.rdb.hash;

import com.dahuaboke.redisx.from.rdb.ParserManager;
import com.dahuaboke.redisx.from.rdb.base.Parser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Desc: Redis >= 7.x版本
 * @Author：zhh
 * @Date：2024/5/21 9:57
 */
public class HashListPackParser implements Parser {

    public Map<byte[], byte[]> parse(ByteBuf byteBuf) {
        Map<byte[], byte[]> map = new HashMap<>();
        byte[] bytes = ParserManager.STRING_00.parse(byteBuf);
        // 创建一个ByteBuf
        ByteBuf buf = Unpooled.buffer();
        // 将byte数组写入ByteBuf
        buf.writeBytes(bytes);
        List<byte[]> list = ParserManager.LISTPACK.parse(buf);
        for (int i = 0; i < list.size(); i += 2) {
            if (i + 1 < list.size()) {
                byte[] key = list.get(i);
                byte[] value = list.get(i + 1);
                map.put(key, value);
            } else {
                break;
            }
        }
        return map;
    }
}
