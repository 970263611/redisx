package com.dahuaboke.redisx.slave.rdb;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Desc: RDB解析类，差分Header
 * @Author：cdl
 * @Date：2024/5/22 14:01
 */
public class RdbData {

    //当前数据的库号
    private long selectDB;

    //当前数据库中一共有多少数据
    private long dataCount;

    //当前数据库中一共有多少带时间的数据
    private long ttlCount;

    //当前数据是当前库的第几个数据库
    private long dataNum;

    //有效时间，时间戳
    private long expireTime;

    //当前数据类型
    private int rdbType;

    //key
    private byte[] key;

    //value
    private Object value;

    public void clear() {
        this.expireTime = -1;
        this.dataNum++;
        this.rdbType = 0;
        this.key = null;
        this.value = null;
    }

    public long getSelectDB() {
        return selectDB;
    }

    public void setSelectDB(long selectDB) {
        this.selectDB = selectDB;
    }

    public long getDataCount() {
        return dataCount;
    }

    public void setDataCount(long dataCount) {
        this.dataCount = dataCount;
    }

    public long getTtlCount() {
        return ttlCount;
    }

    public void setTtlCount(long ttlCount) {
        this.ttlCount = ttlCount;
    }

    public long getDataNum() {
        return dataNum;
    }

    public void setDataNum(long dataNum) {
        this.dataNum = dataNum;
    }

    public long getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(long expireTime) {
        this.expireTime = expireTime;
    }

    public int getRdbType() {
        return rdbType;
    }

    public void setRdbType(int rdbType) {
        this.rdbType = rdbType;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public String toString() {
        String str = "RdbData{" +
                "selectDB=" + selectDB +
                ", dataCount=" + dataCount +
                ", ttlCount=" + ttlCount +
                ", dataNum=" + dataNum +
                ", expireTime=" + expireTime +
                ", rdbType=" + rdbType +
                ", key=" + new String(key);
        String valueStr = "";
        if (value instanceof byte[]) {
            valueStr = new String((byte[]) value);
        } else if (value instanceof List) {
            for (byte[] bytes : ((List<byte[]>) value)) {
                if (valueStr.length() > 0) {
                    valueStr += ",";
                }
                valueStr += new String(bytes);
            }
        } else if (value instanceof Set) {
            for (byte[] bytes : ((Set<byte[]>) value)) {
                if (valueStr.length() > 0) {
                    valueStr += ",";
                }
                valueStr += new String(bytes);
            }
        } else if (value instanceof Map) {
            for (Map.Entry<byte[], byte[]> kAbdV : ((Map<byte[], byte[]>) value).entrySet()) {
                if (valueStr.length() > 0) {
                    valueStr += ",";
                }
                valueStr += new String(kAbdV.getKey());
                valueStr += ":";
                valueStr += new String(kAbdV.getValue());
            }
        }
        str += ", value=" + valueStr + "}";
        return str;
    }
}
