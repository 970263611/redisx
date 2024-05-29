package com.dahuaboke.redisx.slave.rdb;

import com.dahuaboke.redisx.slave.rdb.zset.ZSetEntry;
import io.netty.buffer.ByteBuf;
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

    public ByteBuf parser(RdbHeader rdbHeader) {
        return null;
    }

    public List<String> parser(RdbData rdbData) {
        List result = new LinkedList();
        StringBuilder sb = new StringBuilder();
        int rdbType = rdbData.getRdbType();
        Type type = typeMap.get(rdbType);
        switch (type) {
            case STRING:
                sb.append("set");
                break;
            case LIST:
                sb.append("lpush");
                break;
            case SET:
                sb.append("sadd");
                break;
            case ZSET:
                sb.append("zadd");
                break;
            case HASH:
                sb.append("hset");
                break;
            case MOUDULE:
                break;
            case STREAM:
                break;
            default:
                throw new IllegalArgumentException("Rdb type error");
        }
        sb.append(" ").append(new String(rdbData.getKey()));
        switch (type) {
            case STRING:
                byte[] string = (byte[]) rdbData.getValue();
                sb.append(" ").append(new String(string));
                break;
            case LIST:
                List<byte[]> list = (List<byte[]>) rdbData.getValue();
                for (byte[] bytes : list) {
                    sb.append(" ").append(new String(bytes));
                }
                break;
            case SET:
                Set<byte[]> set = (Set<byte[]>) rdbData.getValue();
                for (byte[] bytes : set) {
                    sb.append(" ").append(new String(bytes));
                }
                break;
            case ZSET:
                Set<ZSetEntry> zset = (Set<ZSetEntry>) rdbData.getValue();
                for (ZSetEntry zSetEntry : zset) {
                    String score = String.valueOf(zSetEntry.getScore());
                    byte[] element = zSetEntry.getElement();
                    sb.append(" ").append(score);
                    sb.append(" ").append(new String(element));
                }
                break;
            case HASH:
                for (Map.Entry<byte[], byte[]> kAbdV : ((Map<byte[], byte[]>) rdbData.getValue()).entrySet()) {
                    byte[] key1 = kAbdV.getKey();
                    byte[] value1 = kAbdV.getValue();
                    sb.append(" ").append(new String(key1));
                    sb.append(" ").append(new String(value1));
                }
                break;
            case MOUDULE:
                break;
            case STREAM:
                break;
            default:
                throw new IllegalArgumentException("Rdb type error");
        }
        result.add(new String(sb));
        long expireTime = rdbData.getExpireTime();
        long lastTime = System.currentTimeMillis() - expireTime;
        ExpiredType expiredType = rdbData.getExpiredType();
        if (ExpiredType.NONE != expiredType) {
            sb = new StringBuilder();
            sb.append("expire");
            sb.append(" ").append(new String(rdbData.getKey()));
            if (ExpiredType.SECOND == expiredType) {
                sb.append(" ").append(lastTime);
            } else if (ExpiredType.MS == expiredType) {
                sb.append(" ").append(lastTime / 1000);
            } else {
                throw new IllegalArgumentException("Rdb type error");
            }
            result.add(new String(sb));
        }
        return result;
    }

//    public String parser(RdbData rdbData) {
//        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
//        int size = 0;
//        int rdbType = rdbData.getRdbType();
//        Type type = typeMap.get(rdbType);
//        switch (type) {
//            case STRING:
//                byteBuf.writeBytes("$3".getBytes());
//                byteBuf.writeBytes(new byte[]{'\r', '\n'});
//                byteBuf.writeBytes("set".getBytes());
//                byteBuf.writeBytes(new byte[]{'\r', '\n'});
//                break;
//            case LIST:
//                byteBuf.writeBytes("$5".getBytes());
//                byteBuf.writeBytes(new byte[]{'\r', '\n'});
//                byteBuf.writeBytes("lpush".getBytes());
//                byteBuf.writeBytes(new byte[]{'\r', '\n'});
//                break;
//            case SET:
//                byteBuf.writeBytes("$4".getBytes());
//                byteBuf.writeBytes(new byte[]{'\r', '\n'});
//                byteBuf.writeBytes("sadd".getBytes());
//                byteBuf.writeBytes(new byte[]{'\r', '\n'});
//                break;
//            case ZSET:
//                byteBuf.writeBytes("$4".getBytes());
//                byteBuf.writeBytes(new byte[]{'\r', '\n'});
//                byteBuf.writeBytes("zadd".getBytes());
//                byteBuf.writeBytes(new byte[]{'\r', '\n'});
//                break;
//            case HASH:
//                byteBuf.writeBytes("$5".getBytes());
//                byteBuf.writeBytes(new byte[]{'\r', '\n'});
//                byteBuf.writeBytes("hmset".getBytes());
//                byteBuf.writeBytes(new byte[]{'\r', '\n'});
//                break;
//            case MOUDULE:
//                break;
//            case STREAM:
//                break;
//            default:
//                throw new IllegalArgumentException("Rdb type error");
//        }
//        byte[] key = rdbData.getKey();
//        byteBuf.writeByte('$');
//        size++;
//        byteBuf.writeBytes(String.valueOf(key.length).getBytes());
//        byteBuf.writeBytes(key);
//        byteBuf.writeBytes(new byte[]{'\r', '\n'});
//        switch (type) {
//            case STRING:
//                byte[] string = (byte[]) rdbData.getValue();
//                byteBuf.writeByte('$');
//                size++;
//                byteBuf.writeBytes(String.valueOf(string.length).getBytes());
//                byteBuf.writeBytes(new byte[]{'\r', '\n'});
//                byteBuf.writeBytes(string);
//                byteBuf.writeBytes(new byte[]{'\r', '\n'});
//                break;
//            case LIST:
//                List<byte[]> list = (List<byte[]>) rdbData.getValue();
//                for (byte[] bytes : list) {
//                    byteBuf.writeByte('$');
//                    size++;
//                    byteBuf.writeBytes(String.valueOf(bytes.length).getBytes());
//                    byteBuf.writeBytes(new byte[]{'\r', '\n'});
//                    byteBuf.writeBytes(bytes);
//                    byteBuf.writeBytes(new byte[]{'\r', '\n'});
//                }
//                break;
//            case SET:
//                Set<byte[]> set = (Set<byte[]>) rdbData.getValue();
//                for (byte[] bytes : set) {
//                    byteBuf.writeByte('$');
//                    size++;
//                    byteBuf.writeBytes(String.valueOf(bytes.length).getBytes());
//                    byteBuf.writeBytes(new byte[]{'\r', '\n'});
//                    byteBuf.writeBytes(bytes);
//                    byteBuf.writeBytes(new byte[]{'\r', '\n'});
//                }
//                break;
//            case ZSET:
//                Set<ZSetEntry> zset = (Set<ZSetEntry>) rdbData.getValue();
//                for (ZSetEntry zSetEntry : zset) {
//                    String score = String.valueOf(zSetEntry.getScore());
//                    byte[] element = zSetEntry.getElement();
//                    byteBuf.writeByte('$');
//                    size++;
//                    byteBuf.writeBytes(String.valueOf(score.length()).getBytes());
//                    byteBuf.writeBytes(new byte[]{'\r', '\n'});
//                    byteBuf.writeBytes(score.getBytes());
//                    byteBuf.writeBytes(new byte[]{'\r', '\n'});
//                    byteBuf.writeByte('$');
//                    size++;
//                    byteBuf.writeBytes(String.valueOf(element.length).getBytes());
//                    byteBuf.writeBytes(new byte[]{'\r', '\n'});
//                    byteBuf.writeBytes(element);
//                    byteBuf.writeBytes(new byte[]{'\r', '\n'});
//                }
//                break;
//            case HASH:
//                for (Map.Entry<byte[], byte[]> kAbdV : ((Map<byte[], byte[]>) rdbData.getValue()).entrySet()) {
//                    byte[] key1 = kAbdV.getKey();
//                    byte[] value1 = kAbdV.getValue();
//                    byteBuf.writeByte('$');
//                    size++;
//                    byteBuf.writeBytes(String.valueOf(key1.length).getBytes());
//                    byteBuf.writeBytes(new byte[]{'\r', '\n'});
//                    byteBuf.writeBytes(key1);
//                    byteBuf.writeBytes(new byte[]{'\r', '\n'});
//                    byteBuf.writeByte('$');
//                    size++;
//                    byteBuf.writeBytes(String.valueOf(value1.length).getBytes());
//                    byteBuf.writeBytes(new byte[]{'\r', '\n'});
//                    byteBuf.writeBytes(value1);
//                    byteBuf.writeBytes(new byte[]{'\r', '\n'});
//                }
//                break;
//            case MOUDULE:
//                break;
//            case STREAM:
//                break;
//            default:
//                throw new IllegalArgumentException("Rdb type error");
//        }
//        ByteBuf head = ByteBufAllocator.DEFAULT.buffer();
//        head.writeByte('*');
//        head.writeBytes(String.valueOf(size).getBytes());
//        head.writeBytes(new byte[]{'\r', '\n'});
//        ByteBuf total = Unpooled.copiedBuffer(head, byteBuf);
//        long expireTime = rdbData.getExpireTime();
//        long lastTime = System.currentTimeMillis() - expireTime;
//        byte[] lastTimeBytes = String.valueOf(lastTime).getBytes();
//        if (expireTime != -1 && (lastTime) > 0) {
//            total.writeBytes(new byte[]{'*', '3'});
//            total.writeBytes(new byte[]{'\r', '\n'});
//            byte[] expireBytes = "expire".getBytes();
//            total.writeByte('$');
//            total.writeBytes(String.valueOf(expireBytes.length).getBytes());
//            total.writeBytes(new byte[]{'\r', '\n'});
//            total.writeBytes(expireBytes);
//            total.writeBytes(new byte[]{'\r', '\n'});
//            total.writeByte('$');
//            total.writeBytes(String.valueOf(key.length).getBytes());
//            total.writeBytes(new byte[]{'\r', '\n'});
//            total.writeBytes(key);
//            total.writeBytes(new byte[]{'\r', '\n'});
//            total.writeByte('$');
//            total.writeBytes(String.valueOf(lastTimeBytes.length).getBytes());
//            total.writeBytes(new byte[]{'\r', '\n'});
//            total.writeBytes(lastTimeBytes);
//            total.writeBytes(new byte[]{'\r', '\n'});
//        }
//        return total.toString(Charset.defaultCharset());
//    }
}
