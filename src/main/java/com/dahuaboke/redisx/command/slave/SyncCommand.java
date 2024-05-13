package com.dahuaboke.redisx.command.slave;

import com.dahuaboke.redisx.command.Command;

/**
 * 2024/5/9 9:37
 * auth: dahua
 * desc:
 */
public class SyncCommand extends Command {

    private String command;

    public SyncCommand(String command) {
        this.command = command;
    }

    public String getCommand() {
        return command;
    }
}
