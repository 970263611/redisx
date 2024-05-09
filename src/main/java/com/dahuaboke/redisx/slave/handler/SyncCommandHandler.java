package com.dahuaboke.redisx.slave.handler;

import com.dahuaboke.redisx.slave.SlaveContext;
import com.dahuaboke.redisx.slave.command.SyncCommand;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/9 17:26
 * auth: dahua
 * desc:
 */
public class SyncCommandHandler extends SimpleChannelInboundHandler<SyncCommand> {

    private static final Logger logger = LoggerFactory.getLogger(SyncCommandHandler.class);
    private SlaveContext slaveContext;

    public SyncCommandHandler(SlaveContext slaveContext) {
        this.slaveContext = slaveContext;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, SyncCommand msg) throws Exception {
        String command = msg.getCommand();
        logger.info("Receive need sync command {}", command);
    }
}
