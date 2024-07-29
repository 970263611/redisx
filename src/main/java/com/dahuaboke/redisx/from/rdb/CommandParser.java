package com.dahuaboke.redisx.from.rdb;

import com.dahuaboke.redisx.from.rdb.stream.Stream;
import com.dahuaboke.redisx.from.rdb.zset.ZSetEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 2024/5/22 15:40
 * auth: dahua
 * desc:
 */
public class CommandParser {

    private static final Logger logger = LoggerFactory.getLogger(CommandParser.class);
    private Map<Integer, Type> typeMap = new HashMap() {{
        put(0x00 & 0xff, Type.STRING);
        put(0x01 & 0xff, Type.LIST);
        put(0x02 & 0xff, Type.SET);
        put(0x03 & 0xff, Type.ZSET);
        put(0x04 & 0xff, Type.HASH);
        put(0x05 & 0xff, Type.ZSET);
        put(0x06 & 0xff, Type.MOUDULE);
        put(0x07 & 0xff, Type.MOUDULE);
        put(0x09 & 0xff, Type.HASH);
        put(0x0a & 0xff, Type.LIST);
        put(0x0b & 0xff, Type.SET);
        put(0x0c & 0xff, Type.ZSET);
        put(0x0d & 0xff, Type.HASH);
        put(0x0e & 0xff, Type.LIST);
        put(0x0f & 0xff, Type.STREAM);
        put(0x10 & 0xff, Type.HASH);
        put(0x11 & 0xff, Type.ZSET);
        put(0x12 & 0xff, Type.LIST);
        put(0x13 & 0xff, Type.STREAM);
        put(0x14 & 0xff, Type.SET);
        put(0x15 & 0xff, Type.STREAM);
    }};

    private enum Type {
        STRING,
        LIST,
        SET,
        ZSET,
        HASH,
        MOUDULE,
        STREAM,
        FUNCTION;
    }

    public List<List<String>> parser(RdbHeader rdbHeader) {
        List<List<String>> result = new LinkedList();
        if (rdbHeader.getFunction() != null && rdbHeader.getFunction().size() > 0) {
            function(result, rdbHeader.getFunction());
        }
        return result;
    }

    public List<List<String>> parser(RdbData rdbData) {
        List<List<String>> result = new LinkedList();
        Type type = typeMap.get(rdbData.getRdbType());
        if (type != null) {
            switch (type) {
                case STRING:
                    string(result, rdbData.getKey(), (byte[]) rdbData.getValue());
                    break;
                case LIST:
                    list(result, rdbData.getKey(), (List<byte[]>) rdbData.getValue());
                    break;
                case SET:
                    set(result, rdbData.getKey(), (Set<byte[]>) rdbData.getValue());
                    break;
                case ZSET:
                    zet(result, rdbData.getKey(), (Set<ZSetEntry>) rdbData.getValue());
                    break;
                case HASH:
                    hash(result, rdbData.getKey(), (Map<byte[], byte[]>) rdbData.getValue());
                    break;
                case MOUDULE:
                    moudule(result, rdbData.getKey(), rdbData.getValue());
                    break;
                case STREAM:
                    stream(result, rdbData.getKey(), (Stream) rdbData.getValue());
                    break;
                default:
                    throw new IllegalArgumentException("Rdb type error");
            }
            long expireTime = rdbData.getExpireTime();
            long lastTime = expireTime - System.currentTimeMillis();
            ExpiredType expiredType = rdbData.getExpiredType();
            if (ExpiredType.NONE != expiredType) {
                List<String> sb = new LinkedList<>();
                sb.add("expire");
                sb.add(new String(rdbData.getKey()));
                if (ExpiredType.SECOND == expiredType) {
                    sb.add(String.valueOf(lastTime));
                } else if (ExpiredType.MS == expiredType) {
                    sb.add(String.valueOf(lastTime / 1000));
                } else {
                    throw new IllegalArgumentException("Rdb type error");
                }
                result.add(sb);
            }
        }
        return result;
    }

    private void string(List<List<String>> list, byte[] key, byte[] value) {
        List<String> sb = new LinkedList<String>() {{
            add("SET");
            add(new String(key));
            add(new String(value));
        }};
        list.add(sb);
    }

    private void list(List<List<String>> list, byte[] key, List<byte[]> value) {
        List<String> sb = new LinkedList<String>() {{
            add("LPUSH");
            add(new String(key));
        }};
        for (byte[] bytes : value) {
            sb.add(new String(bytes));
        }
        list.add(sb);
    }

    private void set(List<List<String>> list, byte[] key, Set<byte[]> value) {
        List<String> sb = new LinkedList<String>() {{
            add("SADD");
            add(new String(key));
        }};
        for (byte[] bytes : value) {
            sb.add(new String(bytes));
        }
        list.add(sb);
    }

    private void zet(List<List<String>> list, byte[] key, Set<ZSetEntry> value) {
        List<String> sb = new LinkedList<String>() {{
            add("ZADD");
            add(new String(key));
        }};
        for (ZSetEntry zSetEntry : value) {
            String score = String.valueOf(zSetEntry.getScore());
            byte[] element = zSetEntry.getElement();
            sb.add(score);
            sb.add(new String(element));
        }
        list.add(sb);
    }

    private void hash(List<List<String>> list, byte[] key, Map<byte[], byte[]> value) {
        List<String> sb = new LinkedList<String>() {{
            add("HSET");
            add(new String(key));
        }};
        for (Map.Entry<byte[], byte[]> kAbdV : value.entrySet()) {
            byte[] key1 = kAbdV.getKey();
            byte[] value1 = kAbdV.getValue();
            sb.add(new String(key1));
            sb.add(new String(value1));
        }
        list.add(sb);
    }

    private void stream(List<List<String>> list, byte[] key, Stream value) {
        String streamName = new String(key);
        if (!value.getEntries().isEmpty()) {
            for (Map.Entry<Stream.ID, Stream.Entry> kandv : value.getEntries().entrySet()) {
                if (kandv.getValue().isDeleted()) {
                    continue;
                }
                List<String> sb = new LinkedList<String>() {{
                    add("XADD");
                    add(streamName);
                    add(kandv.getKey().getMs() + "-" + kandv.getKey().getSeq());
                }};
                for (Map.Entry<byte[], byte[]> entry : kandv.getValue().getFields().entrySet()) {
                    sb.add(new String(entry.getKey()));
                    sb.add(new String(entry.getValue()));
                }
                list.add(sb);
            }
        }
        if (!value.getGroups().isEmpty()) {
            for (int i = 0; i < value.getGroups().size(); i++) {
                Stream.Group group = value.getGroups().get(i);
                List<String> sb = new LinkedList<String>() {{
                    add("XGROUP");
                    add("CREATE");
                    add(streamName);
                    add(new String(group.getName()));
                    add(group.getLastId().getMs() + "-" + group.getLastId().getSeq());
                    add("ENTRIESREAD");
                    add(String.valueOf(group.getEntriesRead()));
                }};
                list.add(sb);
                if (group.getConsumers() != null && group.getConsumers().size() > 0) {
                    for (int m = 0; m < group.getConsumers().size(); m++) {
                        sb.clear();
                        sb.add("XGROUP");
                        sb.add("CREATECONSUMER");
                        sb.add(streamName);
                        sb.add(new String(group.getName()));
                        sb.add(new String(group.getConsumers().get(m).getName()));
                        list.add(sb);
                    }
                }
            }
        }
    }

    private void moudule(List<List<String>> list, byte[] key, Object data) {

    }

    private void function(List<List<String>> list, List<byte[]> value) {
        if (value != null && value.size() > 0) {
            value.forEach(v -> {
                List<String> sb = new LinkedList<String>() {{
                    add("FUNCTION");
                    add("LOAD");
                    add(new String(v));
                }};
                list.add(sb);
            });
        }
    }
}
