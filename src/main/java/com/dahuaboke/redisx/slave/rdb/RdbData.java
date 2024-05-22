package com.dahuaboke.redisx.slave.rdb;

import java.util.Arrays;

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

    public void clear(){
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
        return "RdbData{" +
                "selectDB=" + selectDB +
                ", dataCount=" + dataCount +
                ", ttlCount=" + ttlCount +
                ", dataNum=" + dataNum +
                ", expireTime=" + expireTime +
                ", rdbType=" + rdbType +
              //  ", key=" + new String(key) +
                '}';
    }
}
