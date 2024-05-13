package com.dahuaboke.redisx.forwarder.parse;

import com.dahuaboke.redisx.command.forwarder.ForwardCommand;

import java.util.ArrayList;
import java.util.List;

/**
 * 2024/5/6 11:42
 * auth: dahua
 * desc: 指令解析链条
 */
public final class CommandChain {

    private static class CommandChainHolder {
        private static final CommandChain INSTANCE = new CommandChain();
    }

    public static CommandChain getInstance() {
        return CommandChainHolder.INSTANCE;
    }

    private List<ForwardCommand> commands = new ArrayList();

    public void addCommand(ForwardCommand forwardCommand) {
        commands.add(forwardCommand);
    }

    public List<ForwardCommand> getCommands() {
        return commands;
    }
}
