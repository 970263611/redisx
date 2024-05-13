package com.dahuaboke.redisx.command.forwarder;

import com.dahuaboke.redisx.command.Command;
import com.dahuaboke.redisx.forwarder.ForwardCommandType;
import com.dahuaboke.redisx.forwarder.parse.CommandChain;

/**
 * 2024/5/10 17:00
 * auth: dahua
 * desc:
 */
public abstract class ForwardCommand extends Command {

    public ForwardCommand() {
    }

    public abstract ForwardCommandType getType();

    private void register() {
        CommandChain.getInstance().addCommand(this);
    }

//    public abstract ForwardCommand parse(String command);

//    public abstract void release();
}
