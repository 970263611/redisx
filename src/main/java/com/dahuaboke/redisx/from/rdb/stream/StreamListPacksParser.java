package com.dahuaboke.redisx.from.rdb.stream;

import com.dahuaboke.redisx.from.rdb.ParserManager;
import com.dahuaboke.redisx.from.rdb.base.ListPackParser;
import com.dahuaboke.redisx.from.rdb.base.Parser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.*;

import static com.dahuaboke.redisx.common.Constants.*;

/**
 * @Desc: 解析Stream分为三部分 1. entries 2. aux 3. groups
 * @Author：zhh
 * @Date：2024/5/22 13:47
 */
public class StreamListPacksParser implements Parser {

    ListPackParser listPackParser = new ListPackParser();

    @Override
    public Stream parse(ByteBuf byteBuf) {
        Stream stream = new Stream();
        //流元素
        parseStreamEntries(byteBuf, stream);
        int type = RDB_TYPE_STREAM_LISTPACKS;
        //属性字段值
        parseStreamAux(byteBuf, stream, type);
        //消费者组
        parseStreamGroups(byteBuf, stream, type);
        return stream;
    }

    public void parseStreamEntries(ByteBuf byteBuf, Stream stream) {
        NavigableMap<Stream.ID, Stream.Entry> entries = new TreeMap<>(Stream.ID.COMPARATOR);
        long listPacks = ParserManager.LENGTH.parse(byteBuf).len;
        for (int i = 0; i < listPacks; i++) {
            //key
            byte[] key = ParserManager.STRING_00.parse(byteBuf);
            ByteBuf keyBuf = Unpooled.buffer();
            keyBuf.writeBytes(key);
            Stream.ID baseId = new Stream.ID(keyBuf.readLong(), keyBuf.readLong());
            //value-listPack
            byte[] listPack = ParserManager.STRING_00.parse(byteBuf);
            ByteBuf listPackBuf = Unpooled.buffer();
            listPackBuf.writeBytes(listPack);
            //total-bytes
            listPackBuf.skipBytes(4);
            //num-elements
            listPackBuf.skipBytes(2);
            //elements
            parseEntry(listPackBuf, entries, baseId);
            //尾部结束符
            int end = listPackBuf.readByte() & 0xFF;
            if (end != 255) {
                throw new AssertionError("listPack expect 255 but " + end);
            }
        }
        stream.setEntries(entries);
    }

    /**
     * 解析listPack中的element数据,并放入Stream中的Entry
     * <p>
     * Master entry
     * +-------+---------+------------+---------+--/--+---------+---------+-+
     * | count | deleted | num-fields | field_1 | field_2 | ... | field_N |0|
     * +-------+---------+------------+---------+--/--+---------+---------+-+
     * <p>
     * FLAG
     * +-----+--------+
     * |flags|entry-id|
     * +-----+--------+
     * <p>
     * SAMEFIELD
     * +-------+-/-+-------+--------+
     * |value-1|...|value-N|lp-count|
     * +-------+-/-+-------+--------+
     * <p>
     * NONEFIELD
     * +----------+-------+-------+-/-+-------+-------+--------+
     * |num-fields|field-1|value-1|...|field-N|value-N|lp-count|
     * +----------+-------+-------+-/-+-------+-------+--------+
     *
     * @param listPackBuf
     * @param entries
     * @param baseId
     */
    public void parseEntry(ByteBuf listPackBuf, NavigableMap<Stream.ID, Stream.Entry> entries, Stream.ID baseId) {
        //Master entry
        long count = Long.parseLong(new String(listPackParser.getListPackEntry(listPackBuf)));
        long deleted = Long.parseLong(new String(listPackParser.getListPackEntry(listPackBuf)));
        int numFields = Integer.parseInt(new String(listPackParser.getListPackEntry(listPackBuf)));
        byte[][] tempFields = new byte[numFields][];
        for (int j = 0; j < numFields; j++) {
            tempFields[j] = listPackParser.getListPackEntry(listPackBuf);
        }
        listPackParser.getListPackEntry(listPackBuf);

        long total = count + deleted;
        while (total-- > 0) {
            Map<byte[], byte[]> fields = new HashMap<>();
            //FLAG
            int flag = Integer.parseInt(new String(listPackParser.getListPackEntry(listPackBuf)));
            long ms = Long.parseLong(new String(listPackParser.getListPackEntry(listPackBuf)));
            long seq = Long.parseLong(new String(listPackParser.getListPackEntry(listPackBuf)));
            Stream.ID id = baseId.delta(ms, seq);
            boolean delete = (flag & STREAM_ITEM_FLAG_DELETED) != 0;
            if ((flag & STREAM_ITEM_FLAG_SAMEFIELDS) != 0) {
                //SAMEFIELD
                for (int j = 0; j < numFields; j++) {
                    byte[] value = listPackParser.getListPackEntry(listPackBuf);
                    byte[] field = tempFields[j];
                    fields.put(field, value);
                }
                entries.put(id, new Stream.Entry(id, delete, fields));
            } else {
                //NONEFIELD
                numFields = Integer.parseInt(new String(listPackParser.getListPackEntry(listPackBuf)));
                for (int j = 0; j < numFields; j++) {
                    byte[] field = listPackParser.getListPackEntry(listPackBuf);
                    byte[] value = listPackParser.getListPackEntry(listPackBuf);
                    fields.put(field, value);
                }
                entries.put(id, new Stream.Entry(id, delete, fields));
            }
            //lp-count
            listPackParser.getListPackEntry(listPackBuf);
        }
    }

    public void parseStreamAux(ByteBuf byteBuf, Stream stream, int type) {
        long length = ParserManager.LENGTH.parse(byteBuf).len;
        Stream.ID lastId = new Stream.ID(ParserManager.LENGTH.parse(byteBuf).len, ParserManager.LENGTH.parse(byteBuf).len);
        Stream.ID firstId = null;
        Stream.ID maxDeletedEntryId = null;
        Long entriesAdded = null;
        if (type >= RDB_TYPE_STREAM_LISTPACKS_2) {
            firstId = new Stream.ID(ParserManager.LENGTH.parse(byteBuf).len, ParserManager.LENGTH.parse(byteBuf).len);
            maxDeletedEntryId = new Stream.ID(ParserManager.LENGTH.parse(byteBuf).len, ParserManager.LENGTH.parse(byteBuf).len);
            entriesAdded = ParserManager.LENGTH.parse(byteBuf).len;
        }
        stream.setLastId(lastId);
        if (type >= RDB_TYPE_STREAM_LISTPACKS_2) {
            stream.setFirstId(firstId);
            stream.setMaxDeletedEntryId(maxDeletedEntryId);
            stream.setEntriesAdded(entriesAdded);
        }
        stream.setLength(length);
    }

    public void parseStreamGroups(ByteBuf byteBuf, Stream stream, int type) {
        List<Stream.Group> groups = new ArrayList<>();
        long groupCount = ParserManager.LENGTH.parse(byteBuf).len;
        while (groupCount-- > 0) {
            Stream.Group group = new Stream.Group();
            byte[] groupName = ParserManager.STRING_00.parse(byteBuf);
            Stream.ID groupLastId = new Stream.ID(ParserManager.LENGTH.parse(byteBuf).len, ParserManager.LENGTH.parse(byteBuf).len);
            Long entriesRead = null;
            if (type >= RDB_TYPE_STREAM_LISTPACKS_2) {
                entriesRead = ParserManager.LENGTH.parse(byteBuf).len;
            }
            //Group PEL
            NavigableMap<Stream.ID, Stream.Nack> groupPendingEntries = new TreeMap<>(Stream.ID.COMPARATOR);
            long globalPel = ParserManager.LENGTH.parse(byteBuf).len;
            while (globalPel-- > 0) {
                Stream.ID rawId = new Stream.ID(byteBuf.readLong(), byteBuf.readLong());
                long deliveryTime = byteBuf.readLongLE();
                long deliveryCount = ParserManager.LENGTH.parse(byteBuf).len;
                groupPendingEntries.put(rawId, new Stream.Nack(rawId, null, deliveryTime, deliveryCount));
            }
            //Consumer
            List<Stream.Consumer> consumers = new ArrayList<>();
            long consumerCount = ParserManager.LENGTH.parse(byteBuf).len;
            while (consumerCount-- > 0) {
                Stream.Consumer consumer = new Stream.Consumer();
                byte[] consumerName = ParserManager.STRING_00.parse(byteBuf);
                long seenTime = byteBuf.readLongLE();
                long activeTime = -1;
                if (type >= RDB_TYPE_STREAM_LISTPACKS_3) {
                    activeTime = byteBuf.readLongLE();
                }
                //Consumer PEL
                NavigableMap<Stream.ID, Stream.Nack> consumerPendingEntries = new TreeMap<>(Stream.ID.COMPARATOR);
                long pel = ParserManager.LENGTH.parse(byteBuf).len;
                while (pel-- > 0) {
                    Stream.ID rawId = new Stream.ID(byteBuf.readLong(), byteBuf.readLong());
                    Stream.Nack nack = groupPendingEntries.get(rawId);
                    nack.setConsumer(consumer);
                    consumerPendingEntries.put(rawId, nack);
                }
                consumer.setName(consumerName);
                consumer.setSeenTime(seenTime);
                if (type >= RDB_TYPE_STREAM_LISTPACKS_3) {
                    consumer.setActiveTime(activeTime);
                }
                consumer.setPendingEntries(consumerPendingEntries);
                consumers.add(consumer);
            }
            group.setName(groupName);
            group.setLastId(groupLastId);
            if (type >= RDB_TYPE_STREAM_LISTPACKS_2) {
                group.setEntriesRead(entriesRead);
            }
            group.setPendingEntries(groupPendingEntries);
            group.setConsumers(consumers);
            groups.add(group);
        }
        stream.setGroups(groups);
    }
}
