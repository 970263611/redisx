package com.dahuaboke.redisx.forwarder.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.forwarder.ForwardContext;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/13 10:37
 * auth: dahua
 * desc:
 */
public class SyncCommandListener extends ChannelOutboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(SyncCommandListener.class);
    private ForwardContext forwardContext;

    public SyncCommandListener(ForwardContext forwardContext) {
        this.forwardContext = forwardContext;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        Thread thread = new Thread(() -> {
            String command;
            while ((command = forwardContext.listen()) != null) {
                ctx.writeAndFlush(command);
                logger.debug("Write sync command success");
            }
        });
        thread.setDaemon(true);
        thread.setName(Constant.PROJECT_NAME + "-Forwarder-" + forwardContext.getForwardHost() + ":" + forwardContext.getForwardHost());
        thread.start();
    }
}
