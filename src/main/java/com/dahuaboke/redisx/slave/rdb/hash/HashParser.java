package com.dahuaboke.redisx.slave.rdb.hash;

import com.dahuaboke.redisx.slave.rdb.ParserManager;
import com.dahuaboke.redisx.slave.rdb.base.LengthParser;
import com.dahuaboke.redisx.slave.rdb.base.Parser;
import com.dahuaboke.redisx.slave.rdb.base.StringParser;
import io.netty.buffer.ByteBuf;

import java.util.HashMap;
import java.util.Map;

/**
 * @Desc:
 * @Author：zhh
 * @Date：2024/5/21 9:23
 */
public class HashParser implements Parser {

    public Map<byte[],byte[]> parse(ByteBuf byteBuf){
        long len = ParserManager.LENGTH.parse(byteBuf).len;
        Map<byte[],byte[]> map = new HashMap<>();
        for (int i = 0; i < len; i++) {
            byte[] key = ParserManager.STRING_00.parse(byteBuf);
            byte[] value = ParserManager.STRING_00.parse(byteBuf);
            map.put(key,value);
        }
        return map;
    }
}
