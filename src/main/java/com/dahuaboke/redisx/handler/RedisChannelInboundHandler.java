package com.dahuaboke.redisx.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.CodecException;
import io.netty.handler.codec.redis.*;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/13 15:58
 * auth: dahua
 * desc:
 */
public abstract class RedisChannelInboundHandler extends SimpleChannelInboundHandler<RedisMessage> {

    private static final Logger logger = LoggerFactory.getLogger(RedisChannelInboundHandler.class);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RedisMessage msg) throws Exception {
        channelRead1(ctx, parseArrayRedisMessage(msg));
    }

    public abstract void channelRead1(ChannelHandlerContext ctx, String reply) throws Exception;

    private String parseArrayRedisMessage(RedisMessage msg) {
        if (msg instanceof SimpleStringRedisMessage) {
            return ((SimpleStringRedisMessage) msg).content();
        } else if (msg instanceof ErrorRedisMessage) {
            logger.debug("Receive error message [{}]", ((ErrorRedisMessage) msg).content());
            return ((ErrorRedisMessage) msg).content();
        } else if (msg instanceof IntegerRedisMessage) {
            logger.debug("Receive integer message [{}]", ((IntegerRedisMessage) msg).value());
            return String.valueOf(((IntegerRedisMessage) msg).value());
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
