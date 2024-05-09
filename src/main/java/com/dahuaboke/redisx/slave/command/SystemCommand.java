package com.dahuaboke.redisx.slave.command;

/**
 * 2024/5/8 9:32
 * auth: dahua
 * desc:
 */
public class SystemCommand {

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
