package com.dahuaboke.redisx.slave.rdb.list;

import com.dahuaboke.redisx.slave.rdb.base.LengthParser;
import com.dahuaboke.redisx.slave.rdb.base.ListPackParser;
import com.dahuaboke.redisx.slave.rdb.base.StringParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.LinkedList;
import java.util.List;

import static com.dahuaboke.redisx.Constant.QUICKLIST_NODE_CONTAINER_PACKED;
import static com.dahuaboke.redisx.Constant.QUICKLIST_NODE_CONTAINER_PLAIN;

/**
 * @Desc: quickList2 = list + listPack
 *        Redis >= 7.x版本
 * @Author：zhh
 * @Date：2024/5/20 15:18
 */
public class ListQuickList2Parser {
    LengthParser length = new LengthParser();
    StringParser string = new StringParser();
    ListPackParser listPackParser = new ListPackParser();

    public List<byte[]> parseQuickList2(ByteBuf byteBuf){
        //元素个数
        long len = length.parseLength(byteBuf).len;
        List<byte[]> list = new LinkedList<>();
        for (int i = 0; i < len; i++) {
            long container = length.parseLength(byteBuf).len;
            byte[] bytes = string.parseString(byteBuf);
            if (container == QUICKLIST_NODE_CONTAINER_PLAIN) {
                list.add(bytes);
            } else if (container == QUICKLIST_NODE_CONTAINER_PACKED) {
                // 创建一个ByteBuf
                ByteBuf buf = Unpooled.buffer();
                // 将byte数组写入ByteBuf
                buf.writeBytes(bytes);
                List<byte[]> listByte = listPackParser.parseListPack(buf);
                list.addAll(listByte);
                // 释放ByteBuf的内存
                buf.release();
            } else {
                throw new UnsupportedOperationException(String.valueOf(container));
            }
        }
        return list;
    }
}
