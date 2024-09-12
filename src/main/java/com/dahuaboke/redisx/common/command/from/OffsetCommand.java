package com.dahuaboke.redisx.common.command.from;

import io.netty.buffer.ByteBuf;

/**
 * 2024/5/8 9:33
 * auth: dahua
 * desc:
 */
public class OffsetCommand {

    private String masterId;
    private long offset;
    private ByteBuf in;

    public OffsetCommand(String command) {
        this(command, null);
    }

    public OffsetCommand(String command, ByteBuf in) {
        String[] s = command.split(" ");
        masterId = s[1];
        offset = Long.parseLong(s[2]);
        this.in = in;
    }

    public String getMasterId() {
        return masterId;
    }

    public long getOffset() {
        return offset;
    }

    public ByteBuf getIn() {
        return in;
    }
}
