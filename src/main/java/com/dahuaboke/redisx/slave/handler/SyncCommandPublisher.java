package com.dahuaboke.redisx.slave.handler;

import com.dahuaboke.redisx.command.slave.SyncCommand;
import com.dahuaboke.redisx.slave.SlaveContext;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/9 17:26
 * auth: dahua
 * desc:
 */
public class SyncCommandPublisher extends SimpleChannelInboundHandler<SyncCommand> {

    private static final Logger logger = LoggerFactory.getLogger(SyncCommandPublisher.class);
    private SlaveContext slaveContext;

    public SyncCommandPublisher(SlaveContext slaveContext) {
        this.slaveContext = slaveContext;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, SyncCommand msg) throws Exception {
        String command = msg.getCommand();
        boolean success = slaveContext.publish(command);
        if (success) {
            logger.debug("Success sync command [{}]", command);
        } else {
            logger.error("Sync command [{}] failed", command);
        }
    }
}
