package com.dahuaboke.redisx.handler;

import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.from.FromContext;
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
    private Context context;

    public RedisChannelInboundHandler(Context context) {
        this.context = context;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RedisMessage msg) throws Exception {
        channelRead1(ctx, parseRedisMessage(msg, context));
    }

    public abstract void channelRead1(ChannelHandlerContext ctx, String reply) throws Exception;

    private String parseRedisMessage(RedisMessage msg, Context context) {
        if (msg instanceof SimpleStringRedisMessage) {
            return ((SimpleStringRedisMessage) msg).content();
        } else if (msg instanceof ErrorRedisMessage) {
            logger.warn("Receive error message [{}]", ((ErrorRedisMessage) msg).content());
            return ((ErrorRedisMessage) msg).content();
        } else if (msg instanceof IntegerRedisMessage) {
            return String.valueOf(((IntegerRedisMessage) msg).value());
        } else if (msg instanceof FullBulkStringRedisMessage) {
            FullBulkStringRedisMessage fullMsg = (FullBulkStringRedisMessage) msg;
            if (fullMsg.isNull()) {
                return null;
            }
            return fullMsg.content().toString(CharsetUtil.UTF_8);
        } else if (msg instanceof ArrayRedisMessage) {
            int size = 0, length = 3;
            StringBuilder sb = new StringBuilder();
            for (RedisMessage child : ((ArrayRedisMessage) msg).children()) {
                String c = parseRedisMessage(child, null);
                sb.append(c);
                sb.append(" ");
                size++;
                length += c.length() + String.valueOf(c.length()).length();
            }
            length += String.valueOf(size).length() + 5 * size;
            if (context instanceof FromContext) {
                FromContext fromContext = (FromContext) context;
                long newOffset = fromContext.getOffset() + length;
                fromContext.setOffset(newOffset);
            }
            return sb.delete(sb.length() - 1, sb.length()).toString();
        } else {
            throw new CodecException("Unknown message type: " + msg);
        }
    }
}
