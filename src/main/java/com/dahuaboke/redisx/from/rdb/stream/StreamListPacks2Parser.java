package com.dahuaboke.redisx.from.rdb.stream;

import com.dahuaboke.redisx.from.rdb.base.Parser;
import io.netty.buffer.ByteBuf;

import static com.dahuaboke.redisx.common.Constants.RDB_TYPE_STREAM_LISTPACKS_2;

/**
 * @Desc:
 * @Author：zhh
 * @Date：2024/5/23 9:31
 */
public class StreamListPacks2Parser implements Parser {

    StreamListPacksParser streamListPacksParser = new StreamListPacksParser();

    @Override
    public Stream parse(ByteBuf byteBuf) {
        Stream stream = new Stream();
        //流元素
        streamListPacksParser.parseStreamEntries(byteBuf, stream);
        int type = RDB_TYPE_STREAM_LISTPACKS_2;
        //属性字段值
        streamListPacksParser.parseStreamAux(byteBuf, stream, type);
        //消费者组
        streamListPacksParser.parseStreamGroups(byteBuf, stream, type);
        return stream;
    }
}
