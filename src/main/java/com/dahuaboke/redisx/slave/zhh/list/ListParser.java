package com.dahuaboke.redisx.slave.zhh.list;

import com.dahuaboke.redisx.slave.zhh.LengthParser;
import com.dahuaboke.redisx.slave.zhh.StringParser;
import io.netty.buffer.ByteBuf;
import sun.security.util.Length;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @Desc: list
 * @Author：zhh
 * @Date：2024/5/20 10:56
 */
public class ListParser {

    LengthParser length = new LengthParser();
    StringParser string = new StringParser();

    public List<byte[]> parseList(ByteBuf byteBuf){
        long len = length.parseLength(byteBuf).len;
        List<byte[]> list = new LinkedList<>();
        for (int i = 0; i < len; i++) {
            list.add(string.parseString(byteBuf));
        }
        return list;
    }

}
