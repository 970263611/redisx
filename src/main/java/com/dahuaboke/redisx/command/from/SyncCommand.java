package com.dahuaboke.redisx.command.from;

import com.dahuaboke.redisx.command.Command;

/**
 * 2024/5/9 9:37
 * auth: dahua
 * desc:
 */
public class SyncCommand extends Command {

    private String command;
    private int length;

    public SyncCommand(String command, int length) {
        this.command = command;
        this.length = length;
    }

    public String getCommand() {
        return command;
    }

    public int getLength() {
        return length;
    }
}
