package com.dahuaboke.redisx.slave.handler;

import com.dahuaboke.redisx.exception.CommandException;
import com.dahuaboke.redisx.slave.SyncCommandConst;
import com.dahuaboke.redisx.slave.command.SystemCommand;
import com.dahuaboke.redisx.slave.parser.CommandParser;
import com.dahuaboke.redisx.slave.parser.DefaultCommandParser;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 2024/5/6 15:42
 * auth: dahua
 * desc: 其他公共指令
 */
public class SyncCommandDecoder extends SimpleChannelInboundHandler<ByteBuf> {

    private static final Logger logger = LoggerFactory.getLogger(SyncCommandDecoder.class);

    private CommandParser commandParser;

    public SyncCommandDecoder() {
        this.commandParser = new DefaultCommandParser();
    }

    public SyncCommandDecoder(CommandParser commandParser) {
        this.commandParser = commandParser;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, ByteBuf command) {
        try {
            List<RedisMessage> parse = commandParser.parse(command);
            for (RedisMessage redisMessage : parse) {
//                    System.out.println(redisMessage.toString());
                if (redisMessage instanceof SimpleStringRedisMessage) {
                    SimpleStringRedisMessage message = (SimpleStringRedisMessage) redisMessage;
                    Channel channel = ctx.channel();
                    if (channel.isActive() && channel.pipeline().get(SyncCommandConst.INIT_SYNC_HANDLER_NAME) != null) {
                        ctx.channel().attr(SyncCommandConst.SYNC_REPLY).set(message.content());
                    } else {
                        ctx.fireChannelRead(new SystemCommand(message.content()));
                    }
                }
            }
        } catch (CommandException e) {
            logger.error(String.format("Command cannot be parsed: %s", command.toString(CharsetUtil.UTF_8)), e);
        }
    }
}
