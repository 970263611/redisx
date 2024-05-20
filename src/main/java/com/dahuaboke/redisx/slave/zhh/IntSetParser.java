package com.dahuaboke.redisx.slave.zhh;

import io.netty.buffer.ByteBuf;

import java.util.HashSet;
import java.util.Set;

/**
 * @Desc: intSet解析器
 * @Author：zhh
 * @Date：2024/5/20 10:23
 */
public class IntSetParser {
    public Set<byte[]> parseIntSet(ByteBuf byteBuf){
        Set<byte[]> set = new HashSet<>();
        //编码类型
        int encoding = byteBuf.readIntLE();
        //元素个数
        long contentLength  = byteBuf.readIntLE() & 0xFFFFFFFFL;
        for (int i = 0; i < contentLength; i++) {
            switch (encoding) {
                case 2:
                    String element = String.valueOf(byteBuf.readShortLE());
                    set.add(element.getBytes());
                    break;
                case 4:
                    element = String.valueOf(byteBuf.readIntLE());
                    set.add(element.getBytes());
                    break;
                case 8:
                    element = String.valueOf(byteBuf.readLongLE());
                    set.add(element.getBytes());
                    break;
                default:
                    throw new AssertionError("expect encoding [2,4,8] but:" + encoding);
            }
        }
        return set;
    }
}
