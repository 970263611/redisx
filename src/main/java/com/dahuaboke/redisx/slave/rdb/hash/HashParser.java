package com.dahuaboke.redisx.slave.rdb.hash;

import com.dahuaboke.redisx.slave.rdb.base.LengthParser;
import com.dahuaboke.redisx.slave.rdb.base.StringParser;
import io.netty.buffer.ByteBuf;

import java.util.HashMap;
import java.util.Map;

/**
 * @Desc:
 * @Author：zhh
 * @Date：2024/5/21 9:23
 */
public class HashParser {
    LengthParser length = new LengthParser();
    StringParser string = new StringParser();

    public Map<byte[],byte[]> parseHash(ByteBuf byteBuf){
        long len = length.parseLength(byteBuf).len;
        Map<byte[],byte[]> map = new HashMap<>();
        for (int i = 0; i < len; i++) {
            byte[] key = string.parseString(byteBuf);
            byte[] value = string.parseString(byteBuf);
            map.put(key,value);
        }
        return map;
    }
}
