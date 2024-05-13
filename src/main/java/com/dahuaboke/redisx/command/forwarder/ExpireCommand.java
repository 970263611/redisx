package com.dahuaboke.redisx.command.forwarder;

import com.dahuaboke.redisx.forwarder.ForwardCommandType;

/**
 * 2024/5/10 17:01
 * auth: dahua
 * desc:
 */
public class ExpireCommand extends ForwardCommand {

    @Override
    public ForwardCommandType getType() {
        return ForwardCommandType.EXPIRE;
    }
}
