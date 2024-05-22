package com.dahuaboke.redisx.slave.rdb.zset;

import com.dahuaboke.redisx.slave.rdb.base.LengthParser;
import com.dahuaboke.redisx.slave.rdb.base.Parser;
import com.dahuaboke.redisx.slave.rdb.base.StringParser;
import io.netty.buffer.ByteBuf;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @Desc:  ZSet2
 * @Author：zhh
 * @Date：2024/5/20 17:13
 */
public class ZSet2Parser implements Parser {

    LengthParser length = new LengthParser();
    StringParser string = new StringParser();

    public Set<ZSetEntry> parse(ByteBuf byteBuf){
        Set<ZSetEntry> zset = new LinkedHashSet<>();
        long len = length.parse(byteBuf).len;
        for (int i = 0; i < len; i++) {
            byte[] element = string.parse(byteBuf);
            double score = this.parseBinaryDoubleValue(byteBuf);
            zset.add(new ZSetEntry(element, score));
        }
        return zset;
    }

    private double parseBinaryDoubleValue(ByteBuf byteBuf){
        return Double.longBitsToDouble(byteBuf.readLongLE());
    }

}
