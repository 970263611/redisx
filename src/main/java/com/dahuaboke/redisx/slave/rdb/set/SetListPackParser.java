package com.dahuaboke.redisx.slave.rdb.set;

import com.dahuaboke.redisx.slave.rdb.base.ListPackParser;
import com.dahuaboke.redisx.slave.rdb.base.StringParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @Desc: Set存储的非数字时触发SetListPack
 *        Redis >= 7.x版本
 * @Author：zhh
 * @Date：2024/5/20 15:59
 */
public class SetListPackParser {
    StringParser string = new StringParser();
    ListPackParser listPackParser = new ListPackParser();

    public Set<byte[]> parseSetListPack(ByteBuf byteBuf){
        Set<byte[]> set = new LinkedHashSet<>();
        byte[] bytes = string.parseString(byteBuf);
        // 创建一个ByteBuf
        ByteBuf buf = Unpooled.buffer();
        // 将byte数组写入ByteBuf
        buf.writeBytes(bytes);
        listPackParser.parseListPack(buf).forEach(listByte -> set.add(listByte));
        return set;
    }
}
