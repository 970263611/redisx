package com.dahuaboke.redisx.forwarder.parse;

import com.dahuaboke.redisx.command.forwarder.ForwardCommand;
import com.dahuaboke.redisx.forwarder.ForwardCommandType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 2024/5/10 17:19
 * auth: dahua
 * desc:
 */
public class DefaultCommandParser implements CommandParser {

    private static final Logger logger = LoggerFactory.getLogger(DefaultCommandParser.class);

    @Override
    public ForwardCommand parse(String command) {
//        List<ForwardCommand> commands = CommandChain.getInstance().getCommands();
//        for (ForwardCommand forwardCommand : commands) {
//            ForwardCommandType commandType = forwardCommand.getType();
//            String[] split = command.split(" ");
//            if (split.length > 0) {
//                if (commandType.toString().equalsIgnoreCase(split[0])) {
//                    return forwardCommand.parse(command);
//                }
//            }
//        }
        return null;
    }
}
