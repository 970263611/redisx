package com.dahuaboke.redisx.slave.rdb.hash;

import com.dahuaboke.redisx.slave.rdb.base.Parser;
import com.dahuaboke.redisx.slave.rdb.base.StringParser;
import com.dahuaboke.redisx.slave.rdb.base.ZipListParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Desc:
 * @Author：zhh
 * @Date：2024/5/21 9:57
 */
public class HashZipListParser implements Parser {
    StringParser string = new StringParser();
    ZipListParser zipList = new ZipListParser();

    public Map<byte[],byte[]> parse(ByteBuf byteBuf){
        Map<byte[],byte[]> map = new HashMap<>();
        byte[] bytes = string.parse(byteBuf);
        // 创建一个ByteBuf
        ByteBuf buf = Unpooled.buffer();
        // 将byte数组写入ByteBuf
        buf.writeBytes(bytes);
        List<byte[]> list = zipList.parse(buf);
        for (int i = 0; i < list.size(); i += 2) {
            if(i + 1 < list.size()){
                byte[] key = list.get(i);
                byte[] value = list.get(i + 1);
                map.put(key,value);
            }else {
                break;
            }
        }
        return map;
    }
}
