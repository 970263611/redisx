package com.dahuaboke.redisx.slave.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.CodecException;
import io.netty.handler.codec.redis.*;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/6 15:42
 * auth: dahua
 * desc: 指令后置处理器
 */
public class MessagePostProcessor extends SimpleChannelInboundHandler<RedisMessage> {

    private static final Logger logger = LoggerFactory.getLogger(MessagePostProcessor.class);

    @Override
    public void channelRead0(ChannelHandlerContext ctx, RedisMessage message) {
        try {
            String result = parseArrayRedisMessage(message);
            if (result != null) {
                ctx.fireChannelRead(result);
            }
        } catch (CodecException e) {
            logger.error("Command cannot be parsed: {}", message, e);
        }
    }

    private String parseArrayRedisMessage(RedisMessage msg) {
        if (msg instanceof SimpleStringRedisMessage) {
            return ((SimpleStringRedisMessage) msg).content();
        } else if (msg instanceof ErrorRedisMessage) {
            logger.info("Receive error message: {}", ((ErrorRedisMessage) msg).content());
            return null;
        } else if (msg instanceof IntegerRedisMessage) {
            logger.info("Receive integer message: {}", ((IntegerRedisMessage) msg).value());
            return null;
        } else if (msg instanceof FullBulkStringRedisMessage) {
            FullBulkStringRedisMessage fullMsg = (FullBulkStringRedisMessage) msg;
            if (fullMsg.isNull()) {
                return null;
            }
            return fullMsg.content().toString(CharsetUtil.UTF_8);
        } else if (msg instanceof ArrayRedisMessage) {
            StringBuilder sb = new StringBuilder();
            for (RedisMessage child : ((ArrayRedisMessage) msg).children()) {
                sb.append(parseArrayRedisMessage(child));
                sb.append(" ");
            }
            return new String(sb).substring(0, sb.length() - 1);
        } else {
            throw new CodecException("Unknown message type: " + msg);
        }
    }
}
