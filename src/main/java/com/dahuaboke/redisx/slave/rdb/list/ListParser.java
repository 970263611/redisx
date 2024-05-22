package com.dahuaboke.redisx.slave.rdb.list;

import com.dahuaboke.redisx.slave.rdb.ParserManager;
import com.dahuaboke.redisx.slave.rdb.base.LengthParser;
import com.dahuaboke.redisx.slave.rdb.base.Parser;
import com.dahuaboke.redisx.slave.rdb.base.StringParser;
import io.netty.buffer.ByteBuf;

import java.util.LinkedList;
import java.util.List;

/**
 * @Desc: list
 * @Author：zhh
 * @Date：2024/5/20 10:56
 */
public class ListParser implements Parser {

    public List<byte[]> parse(ByteBuf byteBuf){
        long len = ParserManager.LENGTH.parse(byteBuf).len;
        List<byte[]> list = new LinkedList<>();
        for (int i = 0; i < len; i++) {
            list.add(ParserManager.STRING_00.parse(byteBuf));
        }
        return list;
    }

}
