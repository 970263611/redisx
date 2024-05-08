package com.dahuaboke.redisx.slave.parser;

import java.util.ArrayList;
import java.util.List;

/**
 * 2024/5/6 11:42
 * auth: dahua
 * desc: 指令解析链条
 */
public final class CommandParserChain {

    private static class CommandParseChainHolder {
        private static final CommandParserChain INSTANCE = new CommandParserChain();
    }

    public static CommandParserChain getInstance() {
        return CommandParseChainHolder.INSTANCE;
    }

    private List<CommandParser> commandPars = new ArrayList();

    public void addCommandParse(CommandParser commandParser) {
        commandPars.add(commandParser);
    }

    public List<CommandParser> getCommandParses() {
        return commandPars;
    }
}
