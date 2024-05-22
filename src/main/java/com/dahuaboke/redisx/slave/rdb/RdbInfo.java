package com.dahuaboke.redisx.slave.rdb;

import io.netty.buffer.ByteBuf;

/**
 * @Desc: RDB解析类，差分Header
 * @Author：cdl
 * @Date：2024/5/22 14:01
 */
public class RdbInfo {

    //文件解析完成时，该值改为true
    private boolean end;

    private ByteBuf byteBuf;

    //文件描述信息
    private RdbHeader rdbHeader;

    //文件数据信息，单条
    private RdbData rdbData;

    public RdbInfo(ByteBuf byteBuf) {
        this.byteBuf = byteBuf;
        this.rdbHeader = new RdbHeader();
        this.rdbData = new RdbData();
    }

    public RdbHeader getRdbHeader() {
        return rdbHeader;
    }

    public void setRdbHeader(RdbHeader rdbHeader) {
        this.rdbHeader = rdbHeader;
    }

    public RdbData getRdbData() {
        return rdbData;
    }

    public void setRdbData(RdbData rdbData) {
        this.rdbData = rdbData;
    }

    public ByteBuf getByteBuf() {
        return byteBuf;
    }

    public boolean isEnd() {
        return end;
    }

    public void setEnd(boolean end) {
        this.end = end;
    }
}
