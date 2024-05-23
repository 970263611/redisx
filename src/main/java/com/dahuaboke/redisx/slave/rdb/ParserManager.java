package com.dahuaboke.redisx.slave.rdb;

import com.dahuaboke.redisx.slave.rdb.base.*;
import com.dahuaboke.redisx.slave.rdb.hash.HashListPackParser;
import com.dahuaboke.redisx.slave.rdb.hash.HashParser;
import com.dahuaboke.redisx.slave.rdb.hash.HashZipListParser;
import com.dahuaboke.redisx.slave.rdb.hash.HashZipMapParser;
import com.dahuaboke.redisx.slave.rdb.list.ListParser;
import com.dahuaboke.redisx.slave.rdb.list.ListQuickList2Parser;
import com.dahuaboke.redisx.slave.rdb.list.ListQuickListParser;
import com.dahuaboke.redisx.slave.rdb.list.ListZipListParser;
import com.dahuaboke.redisx.slave.rdb.set.SetIntSetParser;
import com.dahuaboke.redisx.slave.rdb.set.SetListPackParser;
import com.dahuaboke.redisx.slave.rdb.set.SetParser;
import com.dahuaboke.redisx.slave.rdb.stream.Stream;
import com.dahuaboke.redisx.slave.rdb.stream.StreamListPacks2Parser;
import com.dahuaboke.redisx.slave.rdb.stream.StreamListPacks3Parser;
import com.dahuaboke.redisx.slave.rdb.stream.StreamListPacksParser;
import com.dahuaboke.redisx.slave.rdb.zset.ZSetEntry;
import com.dahuaboke.redisx.slave.rdb.zset.ZSetListPackParser;
import com.dahuaboke.redisx.slave.rdb.zset.ZSetParser;
import com.dahuaboke.redisx.slave.rdb.zset.ZSetZipListParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 2024/5/17 15:15
 * auth: dahua
 * desc:
 */
public class ParserManager {

    private static final Logger logger = LoggerFactory.getLogger(ParserManager.class);

    private static Map<Integer, Parser> parserMap = new HashMap<>();

    public static final Parser<LengthParser.Len> LENGTH = new LengthParser();

    public static final Parser<List<byte[]>> LISTPACK = new ListPackParser();

    public static final Parser<List<byte[]>> ZIPLIST = new ZipListParser();

    public static final Parser<Map<byte[], byte[]>> ZIPMAP = new ZipMapParser();

    public static final Parser<Set<byte[]>> INTSET = new IntSetParser();

    public static final Parser<byte[]> STRING_00 = new StringParser();

    public static final Parser<List<byte[]>> LIST_01 = new ListParser();

    public static final Parser<Set<byte[]>> SET_02 = new SetParser();

    public static final Parser<Set<ZSetEntry>> ZSET_03 = new ZSetParser();

    public static final Parser<Map<byte[], byte[]>> HASH_04 = new HashParser();

    public static final Parser<Set<ZSetEntry>> ZSET_2_05 = new ZSetParser();

    //public static final Parser<Map<byte[], byte[]>> MODULE_06 = new ModuleParser();

    //public static final Parser<Map<byte[], byte[]>> MODULE_2_07 = new Module2Parser();

    public static final Parser<Map<byte[], byte[]>> HASH_ZIPMAP_09 = new HashZipMapParser();

    public static final Parser<List<byte[]>> LIST_ZIPLIST_0A = new ListZipListParser();

    public static final Parser<Set<byte[]>> SET_INTSET_0B = new SetIntSetParser();

    public static final Parser<Set<ZSetEntry>> ZSET_ZIPLIST_0C = new ZSetZipListParser();

    public static final Parser<Map<byte[], byte[]>> HASH_ZIPLIST_0D = new HashZipListParser();

    public static final Parser<List<byte[]>> LIST_QUICKLIST_0E = new ListQuickListParser();

    public static final Parser<Stream> STREAM_LISTPACKS_0F = new StreamListPacksParser();

    public static final Parser<Map<byte[], byte[]>> HASH_LISTPACK_10 = new HashListPackParser();

    public static final Parser<Set<ZSetEntry>> ZSET_LISTPACK_11 = new ZSetListPackParser();

    public static final Parser<List<byte[]>> LIST_QUICKLIST_2_12 = new ListQuickList2Parser();

    public static final Parser<Stream> STREAM_LISTPACKS_2_13 = new StreamListPacks2Parser();

    public static final Parser<Set<byte[]>> SET_LISTPACK_14 = new SetListPackParser();

    public static final Parser<Stream> STREAM_LISTPACKS_3_15 = new StreamListPacks3Parser();

    static {
        ParserManager.parserMap.put(0x00 & 0xff, STRING_00);
        ParserManager.parserMap.put(0x01 & 0xff, LIST_01);
        ParserManager.parserMap.put(0x02 & 0xff, SET_02);
        ParserManager.parserMap.put(0x03 & 0xff, ZSET_03);
        ParserManager.parserMap.put(0x04 & 0xff, HASH_04);
        ParserManager.parserMap.put(0x05 & 0xff, ZSET_2_05);
        //ParserManager.parserMap.put(0x06 & 0xff,MODULE_06);
        //ParserManager.parserMap.put(0x07 & 0xff,MODULE_2_07);
        ParserManager.parserMap.put(0x09 & 0xff, HASH_ZIPMAP_09);
        ParserManager.parserMap.put(0x0a & 0xff, LIST_ZIPLIST_0A);
        ParserManager.parserMap.put(0x0b & 0xff, SET_INTSET_0B);
        ParserManager.parserMap.put(0x0c & 0xff, ZSET_ZIPLIST_0C);
        ParserManager.parserMap.put(0x0d & 0xff, HASH_ZIPLIST_0D);
        ParserManager.parserMap.put(0x0e & 0xff, LIST_QUICKLIST_0E);
        ParserManager.parserMap.put(0x0f & 0xff,STREAM_LISTPACKS_0F);
        ParserManager.parserMap.put(0x10 & 0xff, HASH_LISTPACK_10);
        ParserManager.parserMap.put(0x11 & 0xff, ZSET_LISTPACK_11);
        ParserManager.parserMap.put(0x12 & 0xff, LIST_QUICKLIST_2_12);
        ParserManager.parserMap.put(0x13 & 0xff,STREAM_LISTPACKS_2_13);
        ParserManager.parserMap.put(0x14 & 0xff, SET_LISTPACK_14);
        ParserManager.parserMap.put(0x15 & 0xff,STREAM_LISTPACKS_3_15);
    }

    public static Parser getParser(int rdbType) {
        return ParserManager.parserMap.get(rdbType);
    }

}
