package com.dahuaboke.redisx.command.from;

import com.dahuaboke.redisx.command.Command;
import io.netty.buffer.ByteBuf;

/**
 * 2024/5/8 9:33
 * auth: dahua
 * desc:
 */
public class RdbCommand extends Command {

    private ByteBuf in;

    public RdbCommand(ByteBuf in) {
        this.in = in;
    }

    public ByteBuf getIn() {
        return in;
    }
}
