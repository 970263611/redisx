package com.dahuaboke.redisx.common.command.console;

import com.dahuaboke.redisx.common.command.Command;

/**
 * author: dahua
 * date: 2024/8/24 14:05
 */
public class ReplyCommand extends Command {

    private String reply;

    public ReplyCommand(String reply) {
        this.reply = reply;
    }

    public String getReply() {
        return reply;
    }
}
