package com.dahuaboke.redisx.command.forwarder;

import com.dahuaboke.redisx.forwarder.ForwardCommandType;

/**
 * 2024/5/10 16:59
 * auth: dahua
 * desc:
 */
public class ZSetCommand extends ForwardCommand {

    @Override
    public ForwardCommandType getType() {
        return ForwardCommandType.ZSET;
    }
}
