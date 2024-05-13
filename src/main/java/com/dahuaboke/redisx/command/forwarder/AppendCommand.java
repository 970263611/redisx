package com.dahuaboke.redisx.command.forwarder;

import com.dahuaboke.redisx.forwarder.ForwardCommandType;

/**
 * 2024/5/10 17:02
 * auth: dahua
 * desc:
 */
public class AppendCommand extends ForwardCommand {

    @Override
    public ForwardCommandType getType() {
        return ForwardCommandType.APPEND;
    }
}
