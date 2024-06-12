package com.dahuaboke.redisx.from.rdb;

import java.util.ArrayList;
import java.util.List;

/**
 * @Desc: RDB头信息
 * @Author：cdl
 * @Date：2024/5/21 18：00
 */
public class RdbHeader {

    private String ver;//协议版本

    private String redisVer;//redis版本

    private String redisBits;//系统位数

    private String ctime;//文件生成时间

    private String usedMem;//文件占用内存

    private String replStreamDb;//默认数据库

    private String replId;//本次同步40位序号

    private String replOffset;//偏移量

    private String aofBase;

    private List<byte[]> function = new ArrayList<>();//函数列表

    public String getVer() {
        return ver;
    }

    public void setVer(String ver) {
        this.ver = ver;
    }

    public String getRedisVer() {
        return redisVer;
    }

    public void setRedisVer(String redisVer) {
        this.redisVer = redisVer;
    }

    public String getRedisBits() {
        return redisBits;
    }

    public void setRedisBits(String redisBits) {
        this.redisBits = redisBits;
    }

    public String getCtime() {
        return ctime;
    }

    public void setCtime(String ctime) {
        this.ctime = ctime;
    }

    public String getUsedMem() {
        return usedMem;
    }

    public void setUsedMem(String usedMem) {
        this.usedMem = usedMem;
    }

    public String getReplStreamDb() {
        return replStreamDb;
    }

    public void setReplStreamDb(String replStreamDb) {
        this.replStreamDb = replStreamDb;
    }

    public String getReplId() {
        return replId;
    }

    public void setReplId(String replId) {
        this.replId = replId;
    }

    public String getReplOffset() {
        return replOffset;
    }

    public void setReplOffset(String replOffset) {
        this.replOffset = replOffset;
    }

    public String getAofBase() {
        return aofBase;
    }

    public void setAofBase(String aofBase) {
        this.aofBase = aofBase;
    }

    public List<byte[]> getFunction() {
        return function;
    }

    public void setFunction(List<byte[]> function) {
        this.function = function;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder( "RdbHeader{" +
                "ver='" + ver + '\'' +
                ", redisVer='" + redisVer + '\'' +
                ", redisBits='" + redisBits + '\'' +
                ", ctime='" + ctime + '\'' +
                ", usedMem='" + usedMem + '\'' +
                ", replStreamDb='" + replStreamDb + '\'' +
                ", replId='" + replId + '\'' +
                ", replOffset='" + replOffset + '\'' +
                ", aofBase='" + aofBase + '\'' );
        if(function.size() > 0) {
            sb.append(",function=[");
            for (byte[] bytes : function) {
                sb.append("{ ").append(new String(bytes)).append(" }");
            }
            sb.append("]");
        }
        sb.append(" }");
        return sb.toString();
    }
}
