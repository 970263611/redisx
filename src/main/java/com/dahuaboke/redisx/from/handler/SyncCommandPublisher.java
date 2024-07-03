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
    private long unSyncCommandLength = 0;

    public SyncCommandPublisher(FromContext fromContext) {
        this.fromContext = fromContext;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, SyncCommand command) throws Exception {
        int commandLength = command.getCommandLength();
        if (command.isIgnore()) {
            unSyncCommandLength += commandLength;
        } else {
            if (unSyncCommandLength > 0) {
                commandLength += unSyncCommandLength;
                unSyncCommandLength = 0;
            }
            command.setSyncLength(commandLength);
            boolean success = fromContext.publish(command);
            if (success) {
                logger.debug("Success sync command [{}], length [{}]", command.getStringCommand(), commandLength);
            } else {
                logger.error("Sync command [{}] failed, length [{}]", command.getStringCommand(), commandLength);
            }
        }
    }
}
