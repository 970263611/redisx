package com.dahuaboke.redisx.command.forwarder;

import com.dahuaboke.redisx.forwarder.ForwardCommandType;

/**
 * 2024/5/10 17:03
 * auth: dahua
 * desc:
 */
public class IncrCommand extends ForwardCommand {

    @Override
    public ForwardCommandType getType() {
        return ForwardCommandType.INCR;
    }
}
