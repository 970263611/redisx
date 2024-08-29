package com.dahuaboke.redisx.handler;

import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.common.Constants;
import com.dahuaboke.redisx.common.command.from.SyncCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.CodecException;
import io.netty.handler.codec.redis.*;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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
        SyncCommand syncCommand = new SyncCommand(context, true);
        syncCommand.setRedisMessage(msg);
        parseRedisMessage(msg, syncCommand);
        channelRead1(ctx, syncCommand);
    }

    public void channelRead1(ChannelHandlerContext ctx, SyncCommand syncCommand) throws Exception {
        channelRead2(ctx, syncCommand.getStringCommand());
    }

    public abstract void channelRead2(ChannelHandlerContext ctx, String reply) throws Exception;

    private void parseRedisMessage(RedisMessage msg, SyncCommand syncCommand) {
        if (msg instanceof SimpleStringRedisMessage) {
            String content = ((SimpleStringRedisMessage) msg).content();
            syncCommand.appendCommand(content);
            syncCommand.appendLength(1 + String.valueOf(content.length()).length() + 2 + content.getBytes().length + 2);
        } else if (msg instanceof ErrorRedisMessage) {
            String err = ((ErrorRedisMessage) msg).content();
            logger.warn("Receive error message [{}]", err);
            syncCommand.appendCommand(Constants.ERROR_REPLY_PREFIX + err);
        } else if (msg instanceof IntegerRedisMessage) {
            syncCommand.appendCommand(String.valueOf(((IntegerRedisMessage) msg).value()));
        } else if (msg instanceof FullBulkStringRedisMessage) {
            FullBulkStringRedisMessage fullMsg = (FullBulkStringRedisMessage) msg;
            if (fullMsg.isNull()) {
                syncCommand.appendCommand("(null)");
                return;
            }
            ByteBuf content = fullMsg.content();
            syncCommand.appendCommand(content.toString(CharsetUtil.UTF_8));
            syncCommand.appendLength(1 + String.valueOf(content.readableBytes()).length() + 2 + content.readableBytes() + 2);
        } else if (msg instanceof ArrayRedisMessage) {
            List<RedisMessage> children = ((ArrayRedisMessage) msg).children();
            syncCommand.appendLength(1 + String.valueOf(children.size()).length() + 2);
            for (RedisMessage child : ((ArrayRedisMessage) msg).children()) {
                parseRedisMessage(child, syncCommand);
            }
        } else {
            throw new CodecException("Unknown message type: " + msg);
        }
    }
}
