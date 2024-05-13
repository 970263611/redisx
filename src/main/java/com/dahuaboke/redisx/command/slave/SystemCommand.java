package com.dahuaboke.redisx.command.slave;

import com.dahuaboke.redisx.command.Command;

/**
 * 2024/5/8 9:32
 * auth: dahua
 * desc:
 */
public class SystemCommand extends Command {

    protected Object command;

    public SystemCommand() {
    }

    public SystemCommand(Object command) {
        this.command = command;
    }

    public Object getCommand() {
        return command;
    }
}
