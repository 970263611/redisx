package com.dahuaboke.redisx.slave.rdb.set;

import com.dahuaboke.redisx.slave.rdb.base.LengthParser;
import com.dahuaboke.redisx.slave.rdb.base.Parser;
import com.dahuaboke.redisx.slave.rdb.base.StringParser;
import io.netty.buffer.ByteBuf;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @Desc: Set存储的非数字时触发Set
 *        Redis < 7.x 版本
 * @Author：zhh
 * @Date：2024/5/20 15:47
 */
public class SetParser implements Parser {
    LengthParser length = new LengthParser();
    StringParser string = new StringParser();

    public Set<byte[]> parse(ByteBuf byteBuf){
        long len = length.parse(byteBuf).len;
        Set<byte[]> set = new LinkedHashSet<>();
        for (int i = 0; i < len; i++) {
            byte[] element = string.parse(byteBuf);
            set.add(element);
        }
        return set;
    }
}
