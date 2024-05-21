package com.dahuaboke.redisx.slave.zhh.zset;

import com.dahuaboke.redisx.slave.zhh.LengthParser;
import com.dahuaboke.redisx.slave.zhh.StringParser;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @Desc:  ZSet2
 * @Author：zhh
 * @Date：2024/5/20 17:13
 */
public class ZSet2Parser {

    LengthParser length = new LengthParser();
    StringParser string = new StringParser();

    public Set<ZSetEntry> parseZSet2(ByteBuf byteBuf){
        Set<ZSetEntry> zset = new LinkedHashSet<>();
        long len = length.parseLength(byteBuf).len;
        for (int i = 0; i < len; i++) {
            byte[] element = string.parseString(byteBuf);
            double score = this.parseBinaryDoubleValue(byteBuf);
            zset.add(new ZSetEntry(element, score));
        }
        return zset;
    }

    private double parseBinaryDoubleValue(ByteBuf byteBuf){
        return Double.longBitsToDouble(byteBuf.readLongLE());
    }

}
