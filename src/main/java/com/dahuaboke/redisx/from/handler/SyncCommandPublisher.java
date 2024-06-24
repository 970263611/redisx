package com.dahuaboke.redisx.from.handler;

import com.dahuaboke.redisx.command.from.SyncCommand;
import com.dahuaboke.redisx.from.FromContext;
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
    private FromContext fromContext;

    public SyncCommandPublisher(FromContext fromContext) {
        this.fromContext = fromContext;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, SyncCommand msg) throws Exception {
        String command = msg.getCommand();
        int length = msg.getLength();
        boolean success = fromContext.publish(command, length);
        if (success) {
            logger.debug("Success sync command [{}], length [{}]", command, length);
        } else {
            logger.error("Sync command [{}] failed, length [{}]", command, length);
        }
    }
}
