package com.dahuaboke.redisx.command.forwarder;

import com.dahuaboke.redisx.forwarder.ForwardCommandType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/10 17:12
 * auth: dahua
 * desc:
 */
public class SmoveCommand extends ForwardCommand {

    private static final Logger logger = LoggerFactory.getLogger(SmoveCommand.class);

    @Override
    public ForwardCommandType getType() {
        return ForwardCommandType.SMOVE;
    }
}
