package com.dahuaboke.redisx.from.rdb.set;

import com.dahuaboke.redisx.from.rdb.ParserManager;
import com.dahuaboke.redisx.from.rdb.base.Parser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @Desc: Set存储的非数字时触发SetListPack
 * Redis >= 7.x版本
 * @Author：zhh
 * @Date：2024/5/20 15:59
 */
public class SetListPackParser implements Parser {

    public Set<byte[]> parse(ByteBuf byteBuf) {
        Set<byte[]> set = new LinkedHashSet<>();
        byte[] bytes = ParserManager.STRING_00.parse(byteBuf);
        // 创建一个ByteBuf
        ByteBuf buf = Unpooled.buffer();
        // 将byte数组写入ByteBuf
        buf.writeBytes(bytes);
        ParserManager.LISTPACK.parse(buf).forEach(listByte -> set.add(listByte));
        return set;
    }
}
