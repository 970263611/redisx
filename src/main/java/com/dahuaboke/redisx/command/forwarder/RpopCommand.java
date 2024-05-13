package com.dahuaboke.redisx.command.forwarder;

import com.dahuaboke.redisx.forwarder.ForwardCommandType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/10 17:08
 * auth: dahua
 * desc:
 */
public class RpopCommand extends ForwardCommand {

    private static final Logger logger = LoggerFactory.getLogger(RpopCommand.class);

    @Override
    public ForwardCommandType getType() {
        return ForwardCommandType.RPOP;
    }
}
