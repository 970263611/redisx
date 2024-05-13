package com.dahuaboke.redisx.command.forwarder;

import com.dahuaboke.redisx.forwarder.ForwardCommandType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/10 17:11
 * auth: dahua
 * desc:
 */
public class SremCommand extends ForwardCommand {

    private static final Logger logger = LoggerFactory.getLogger(SremCommand.class);

    @Override
    public ForwardCommandType getType() {
        return ForwardCommandType.SREM;
    }
}
