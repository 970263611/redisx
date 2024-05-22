package com.dahuaboke.redisx.slave.rdb.zset;

import com.dahuaboke.redisx.slave.rdb.base.LengthParser;
import com.dahuaboke.redisx.slave.rdb.base.StringParser;
import io.netty.buffer.ByteBuf;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @Desc:
 * @Author：zhh
 * @Date：2024/5/20 17:13
 */
public class ZSetParser {

    LengthParser length = new LengthParser();
    StringParser string = new StringParser();

    public Set<ZSetEntry> parseZSet(ByteBuf byteBuf){
        Set<ZSetEntry> zset = new LinkedHashSet<>();
        long len = length.parseLength(byteBuf).len;
        for (int i = 0; i < len; i++) {
            byte[] element = string.parseString(byteBuf);
            double score = this.parseDoubleValue(byteBuf);
            zset.add(new ZSetEntry(element, score));
        }
        return zset;
    }
    public double parseDoubleValue(ByteBuf byteBuf){
        int len = byteBuf.readByte() & 0xFF;
        switch (len) {
            case 255:
                return Double.NEGATIVE_INFINITY;
            case 254:
                return Double.POSITIVE_INFINITY;
            case 253:
                return Double.NaN;
            default:
                byte[] bytes = new byte[len];
                byteBuf.readBytes(byteBuf);
                return Double.valueOf(new String(bytes));
        }
    }

}
