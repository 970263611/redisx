package com.dahuaboke.redisx.slave.parser;

import com.dahuaboke.redisx.exception.CommandException;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 2024/5/6 11:14
 * auth: dahua
 * desc:
 */
public class DefaultCommandParser implements CommandParser {

    private static final Logger logger = LoggerFactory.getLogger(DefaultCommandParser.class);

    public DefaultCommandParser() {
        /**
         * 绑定通用解析器
         */
        new CommonCommandParser(true).bind();
    }

    @Override
    public List<RedisMessage> parse(ByteBuf byteBuf) throws CommandException {
        List<CommandParser> commandParsers = CommandParserChain.getInstance().getCommandParses();
        for (CommandParser commandParser : commandParsers) {
            byte type = byteBuf.getByte(0);
            if (commandParser.matching(type)) {
                return commandParser.parse(byteBuf);
            }
        }
        throw new CommandException();
    }

    @Override
    public boolean matching(byte b) {
        return false;
    }

    /**
     * 空逻辑
     */
    @Override
    public void bind() {
    }
}
