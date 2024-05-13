package com.dahuaboke.redisx.forwarder.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.forwarder.ForwarderContext;
import com.dahuaboke.redisx.handler.RedisChannelInboundHandler;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/13 10:37
 * auth: dahua
 * desc:
 */
public class SyncCommandListener extends RedisChannelInboundHandler {

    private static final Logger logger = LoggerFactory.getLogger(SyncCommandListener.class);
    private ForwarderContext forwarderContext;

    public SyncCommandListener(ForwarderContext forwarderContext) {
        this.forwarderContext = forwarderContext;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        Thread thread = new Thread(() -> {
            for (; ; ) {
                String command = forwarderContext.listen();
                if (command != null) {
                    ctx.writeAndFlush(command);
                    logger.debug("Write sync command success [{}]", command);
                }
            }
        });
        thread.setDaemon(true);
        thread.setName(Constant.PROJECT_NAME + "-Forwarder-" + forwarderContext.getForwardHost() + ":" + forwarderContext.getForwardPort());
        thread.start();
    }

    @Override
    public void channelRead1(ChannelHandlerContext ctx, String reply) throws Exception {
        logger.debug("Receive redis reply [{}]", reply);
    }
}
