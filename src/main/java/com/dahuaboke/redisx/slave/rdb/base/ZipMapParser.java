package com.dahuaboke.redisx.slave.rdb.base;

import io.netty.buffer.ByteBuf;
import java.util.HashMap;
import java.util.Map;

/**
 * @Desc: zipMap解析器
 * @Author：zhh
 * @Date：2024/5/20 9:36
 */
public class ZipMapParser implements Parser{

    public Map<byte[],byte[]> parse(ByteBuf byteBuf){
        HashMap<byte[],byte[]> map = new HashMap<>();
        //不使用zmlen做遍历
        int zmlen = byteBuf.readByte() & 0xFF;
        while (true) {
            //key
            int keyLength = zmElementLen(byteBuf);
            //使用FF做标志位跳出遍历
            if (keyLength == 255) {
                break;
            }
            byte[] key = new byte[keyLength];
            byteBuf.readBytes(key);
            //value
            int valueLength = zmElementLen(byteBuf);
            if (valueLength == 255) {
                //value is null
                map.put(key, null);
                break;
            }
            //free
            int freeLength = byteBuf.readByte() & 0xFF;
            byte[] value = new byte[valueLength];
            byteBuf.readBytes(value);
            byteBuf.skipBytes(freeLength);
            map.put(key,value);
        }
        return map;
    }
    public int zmElementLen(ByteBuf byteBuf) {
        int len = byteBuf.readByte() & 0xFF;
        if (len >= 0 && len <= 253) {
            return len;
        } else if (len == 254) {
            //大端格式
            return byteBuf.readInt();
        } else {
            return len;
        }
    }
}
