package com.dahuaboke.redisx.slave.rdb.zset;

import com.dahuaboke.redisx.slave.rdb.ParserManager;
import com.dahuaboke.redisx.slave.rdb.base.Parser;
import io.netty.buffer.ByteBuf;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @Desc: ZSet2
 * @Author：zhh
 * @Date：2024/5/20 17:13
 */
public class ZSet2Parser implements Parser {

    public Set<ZSetEntry> parse(ByteBuf byteBuf) {
        Set<ZSetEntry> zset = new LinkedHashSet<>();
        long len = ParserManager.LENGTH.parse(byteBuf).len;
        for (int i = 0; i < len; i++) {
            byte[] element = ParserManager.STRING_00.parse(byteBuf);
            double score = this.parseBinaryDoubleValue(byteBuf);
            zset.add(new ZSetEntry(element, score));
        }
        return zset;
    }

    private double parseBinaryDoubleValue(ByteBuf byteBuf) {
        return Double.longBitsToDouble(byteBuf.readLongLE());
    }

}
