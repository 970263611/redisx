package com.dahuaboke.redisx.slave.rdb.stream;

import com.dahuaboke.redisx.slave.rdb.base.Parser;
import io.netty.buffer.ByteBuf;

import static com.dahuaboke.redisx.Constant.RDB_TYPE_STREAM_LISTPACKS_3;

/**
 * @Desc: Redis >= 7.x版本
 * @Author：zhh
 * @Date：2024/5/23 9:32
 */
public class StreamListPacks3Parser implements Parser {

    StreamListPacksParser streamListPacksParser = new StreamListPacksParser();

    @Override
    public Stream parse(ByteBuf byteBuf) {
        Stream stream = new Stream();
        //流元素
        streamListPacksParser.parseStreamEntries(byteBuf, stream);
        int type = RDB_TYPE_STREAM_LISTPACKS_3;
        //属性字段值
        streamListPacksParser.parseStreamAux(byteBuf, stream, type);
        //消费者组
        streamListPacksParser.parseStreamGroups(byteBuf, stream, type);
        return stream;
    }
}
