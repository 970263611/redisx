package com.dahuaboke.redisx.slave.rdb;

import io.netty.buffer.ByteBuf;

public class RdbInfo {

    private boolean end;

    private ByteBuf byteBuf;

    private RdbHeader rdbHeader;

    private RdbData rdbData;

    public RdbInfo(ByteBuf byteBuf){
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
