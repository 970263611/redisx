package com.dahuaboke.redisx.slave.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/9 9:37
 * auth: dahua
 * desc:
 */
public class SyncCommand {

    private static final Logger logger = LoggerFactory.getLogger(SyncCommand.class);

    private String command;

    public SyncCommand(String command) {
        this.command = command;
    }

    public String getCommand() {
        return command;
    }
}
