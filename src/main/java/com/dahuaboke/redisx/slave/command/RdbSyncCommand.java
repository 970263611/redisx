package com.dahuaboke.redisx.slave.command;

import io.netty.buffer.ByteBuf;

/**
 * 2024/5/8 9:33
 * auth: dahua
 * desc:
 */
public class RdbSyncCommand {

    private ByteBuf in;

    public RdbSyncCommand(ByteBuf in) {
        this.in = in;
    }

    public ByteBuf getIn() {
        return in;
    }
}
