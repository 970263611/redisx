package com.dahuaboke.redisx.slave.rdb;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.slave.rdb.base.Parser;
import com.dahuaboke.redisx.slave.rdb.stream.Stream;
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

    public List<String> parser(RdbHeader rdbHeader) {
        List<String> result = new LinkedList();
        if(rdbHeader.getFunction() != null && rdbHeader.getFunction().size() > 0){
            function(result,rdbHeader.getFunction());
        }
        return result;
    }

    public List<String> parser(RdbData rdbData) {
        List<String> result = new LinkedList();
        switch (typeMap.get(rdbData.getRdbType())) {
            case STRING:
                string(result,rdbData.getKey(), (byte[]) rdbData.getValue());
                break;
            case LIST:
                list(result,rdbData.getKey(), (List<byte[]>) rdbData.getValue());
                break;
            case SET:
                set(result,rdbData.getKey(), (Set<byte[]>) rdbData.getValue());
                break;
            case ZSET:
                zet(result,rdbData.getKey(), (Set<ZSetEntry>) rdbData.getValue());
                break;
            case HASH:
                hash(result,rdbData.getKey(), (Map<byte[], byte[]>) rdbData.getValue());
                break;
            case MOUDULE:
                moudule(result,rdbData.getKey(), rdbData.getValue());
                break;
            case STREAM:
                stream(result,rdbData.getKey(), (Stream) rdbData.getValue());
                break;
            default:
                throw new IllegalArgumentException("Rdb type error");
        }
        long expireTime = rdbData.getExpireTime();
        long lastTime = System.currentTimeMillis() - expireTime;
        ExpiredType expiredType = rdbData.getExpiredType();
        if (ExpiredType.NONE != expiredType) {
            StringBuilder sb = new StringBuilder();
            sb.append("expire");
            sb.append(Constant.STR_SPACE).append(new String(rdbData.getKey()));
            if (ExpiredType.SECOND == expiredType) {
                sb.append(Constant.STR_SPACE).append(lastTime);
            } else if (ExpiredType.MS == expiredType) {
                sb.append(Constant.STR_SPACE).append(lastTime / 1000);
            } else {
                throw new IllegalArgumentException("Rdb type error");
            }
            result.add(new String(sb));
        }
        return result;
    }

    private void string(List<String> list,byte[] key,byte[] value){
        StringBuilder sb = new StringBuilder();
        sb.append("SET");
        sb.append(Constant.STR_SPACE).append(new String(key));
        sb.append(Constant.STR_SPACE).append(new String(value));
        list.add(sb.toString());
    }

    private void list(List<String> list,byte[] key,List<byte[]> value){
        StringBuilder sb = new StringBuilder();
        sb.append("LPUSH");
        sb.append(Constant.STR_SPACE).append(new String(key));
        for (byte[] bytes : value) {
            sb.append(Constant.STR_SPACE).append(new String(bytes));
        }
        list.add(sb.toString());
    }

    private void set(List<String> list,byte[] key,Set<byte[]> value){
        StringBuilder sb = new StringBuilder();
        sb.append("SADD");
        sb.append(Constant.STR_SPACE).append(new String(key));
        for (byte[] bytes : value) {
            sb.append(Constant.STR_SPACE).append(new String(bytes));
        }
        list.add(sb.toString());
    }

    private void zet(List<String> list,byte[] key,Set<ZSetEntry> value){
        StringBuilder sb = new StringBuilder();
        sb.append("ZADD");
        sb.append(Constant.STR_SPACE).append(new String(key));
        for (ZSetEntry zSetEntry : value) {
            String score = String.valueOf(zSetEntry.getScore());
            byte[] element = zSetEntry.getElement();
            sb.append(Constant.STR_SPACE).append(score);
            sb.append(Constant.STR_SPACE).append(new String(element));
        }
        list.add(sb.toString());
    }

    private void hash(List<String> list,byte[] key,Map<byte[], byte[]> value){
        StringBuilder sb = new StringBuilder();
        sb.append("HSET");
        sb.append(Constant.STR_SPACE).append(new String(key));
        for (Map.Entry<byte[], byte[]> kAbdV : value.entrySet()) {
            byte[] key1 = kAbdV.getKey();
            byte[] value1 = kAbdV.getValue();
            sb.append(Constant.STR_SPACE).append(new String(key1));
            sb.append(Constant.STR_SPACE).append(new String(value1));
        }
        list.add(sb.toString());
    }

    private void stream(List<String> list, byte[] key, Stream value){
        if(!value.getEntries().isEmpty()){
            for(Map.Entry<Stream.ID, Stream.Entry> kandv: value.getEntries().entrySet()){
                if(kandv.getValue().isDeleted()){
                    continue;
                }
                StringBuilder sb = new StringBuilder();
                sb.append("XADD");
                sb.append(Constant.STR_SPACE).append(new String(key));
                sb.append(Constant.STR_SPACE).append(kandv.getKey().getMs()).append("-").append(kandv.getKey().getSeq());
                for(Map.Entry<byte[],byte[]> entry : kandv.getValue().getFields().entrySet()){
                    sb.append(Constant.STR_SPACE).append(new String(entry.getKey()));
                    sb.append(Constant.STR_SPACE).append(new String(entry.getValue()));
                }
                list.add(sb.toString());
            }
        }
        if(!value.getGroups().isEmpty()){
            for(int i=0;i < value.getGroups().size();i++){
                Stream.Group group = value.getGroups().get(i);
                StringBuilder sb = new StringBuilder();
                sb.append("XGROUP CREATE");
                sb.append(Constant.STR_SPACE).append(new String(group.getName()));
                if(group.getConsumers() != null && group.getConsumers().size() > 0){
                    for(int m=0;m < group.getConsumers().size();m++){
                        sb.append(Constant.STR_SPACE).append(new String(group.getConsumers().get(m).getName()));
                    }
                }
                sb.append(Constant.STR_SPACE).append(group.getLastId().getMs()).append("-").append(group.getLastId().getMs());
                list.add(sb.toString());
            }
        }
    }

    private void moudule(List<String> list,byte[] key,Object data){

    }

    private void function(List<String> list,List<byte[]> value){
        if(value != null && value.size() > 0){
            value.forEach(v -> {
                StringBuilder sb = new StringBuilder();
                sb.append("FUNCTION LOAD");
                sb.append(Constant.STR_SPACE).append(new String(v));
                list.add(sb.toString());
            });
        }
    }

}
