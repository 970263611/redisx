package com.dahuaboke.redisx.slave.handler;

import com.dahuaboke.redisx.slave.SlaveConst;
import com.dahuaboke.redisx.slave.command.PingCommand;
import com.dahuaboke.redisx.slave.command.SyncCommand;
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
        if (channel.isActive() && channel.pipeline().get(SlaveConst.INIT_SYNC_HANDLER_NAME) != null) {
            ctx.channel().attr(SlaveConst.SYNC_REPLY).set(msg);
        } else if (SlaveConst.PING_COMMAND.equalsIgnoreCase(msg)) {
            ctx.fireChannelRead(new PingCommand());
        } else {
            ctx.fireChannelRead(new SyncCommand(msg));
        }
    }
}