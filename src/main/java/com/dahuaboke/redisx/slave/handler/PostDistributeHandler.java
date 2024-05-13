package com.dahuaboke.redisx.slave.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.command.slave.PingCommand;
import com.dahuaboke.redisx.command.slave.SyncCommand;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/9 10:11
 * auth: dahua
 * desc:
 */
public class PostDistributeHandler extends SimpleChannelInboundHandler<String> {

    private static final Logger logger = LoggerFactory.getLogger(PostDistributeHandler.class);

    @Override
    public void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        Channel channel = ctx.channel();
        if (channel.isActive() && channel.pipeline().get(Constant.INIT_SYNC_HANDLER_NAME) != null) {
            ctx.channel().attr(Constant.SYNC_REPLY).set(msg);
        } else if (Constant.PING_COMMAND.equalsIgnoreCase(msg)) {
            ctx.fireChannelRead(new PingCommand());
        } else {
            ctx.fireChannelRead(new SyncCommand(msg));
        }
    }
}