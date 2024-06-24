package com.dahuaboke.redisx.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.Context;
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

    private long notSyncCommandLength;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RedisMessage msg) throws Exception {
        String[] replys = parseRedisMessage(msg);
        if (replys != null) {
            channelRead1(ctx, replys);
        }
    }

    protected void channelRead1(ChannelHandlerContext ctx, String[] replys) throws Exception {
        channelRead2(ctx, replys[0]);
    }

    public abstract void channelRead2(ChannelHandlerContext ctx, String reply) throws Exception;

    private String[] parseRedisMessage(RedisMessage msg) {
        String[] result = new String[1];
        if (msg instanceof SimpleStringRedisMessage) {
            result[0] = ((SimpleStringRedisMessage) msg).content();
            return result;
        } else if (msg instanceof ErrorRedisMessage) {
            logger.warn("Receive error message [{}]", ((ErrorRedisMessage) msg).content());
            result[0] = Constant.ERROR_REPLY_PREFIX + ((ErrorRedisMessage) msg).content();
            return result;
        } else if (msg instanceof IntegerRedisMessage) {
            result[0] = String.valueOf(((IntegerRedisMessage) msg).value());
            return result;
        } else if (msg instanceof FullBulkStringRedisMessage) {
            FullBulkStringRedisMessage fullMsg = (FullBulkStringRedisMessage) msg;
            if (fullMsg.isNull()) {
                return null;
            }
            result[0] = fullMsg.content().toString(CharsetUtil.UTF_8);
            return result;
        } else if (msg instanceof ArrayRedisMessage) {
            Integer size = 0, length = 3;
            StringBuilder sb = new StringBuilder();
            for (RedisMessage child : ((ArrayRedisMessage) msg).children()) {
                String[] c = parseRedisMessage(child);
                sb.append(c[0]);
                sb.append(" ");
                size++;
                length += c[0].length() + String.valueOf(c[0].length()).length();
            }
            length += String.valueOf(size).length() + 5 * size;
            result = new String[2];
            result[0] = sb.delete(sb.length() - 1, sb.length()).toString();
            if (result[0].toUpperCase().startsWith(Constant.SELECT)) {
                if (!context.isFromIsCluster() && !context.isToIsCluster()) {
                    result[1] = String.valueOf(length + notSyncCommandLength);
                    notSyncCommandLength = 0;
                    return result;
                } else {
                    notSyncCommandLength += length;
                    return null;
                }
            }
            if (Constant.PING_COMMAND.equalsIgnoreCase(result[0]) ||
                    result[0].toUpperCase().startsWith(Constant.SELECT)) {
                notSyncCommandLength += length;
                return null;
            }
            result[1] = String.valueOf(length + notSyncCommandLength);
            notSyncCommandLength = 0;
            return result;
        } else {
            throw new CodecException("Unknown message type: " + msg);
        }
    }
}
