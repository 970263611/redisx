package com.dahuaboke.redisx.slave.command;

/**
 * 2024/5/8 9:32
 * auth: dahua
 * desc:
 */
public class SystemCommand {

    protected String command;

    public SystemCommand() {
    }

    public SystemCommand(String command) {
        this.command = command;
    }

    public String getCommand() {
        return command;
    }
}
