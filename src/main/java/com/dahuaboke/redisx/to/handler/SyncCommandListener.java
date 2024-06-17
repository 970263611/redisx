package com.dahuaboke.redisx.to.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.handler.RedisChannelInboundHandler;
import com.dahuaboke.redisx.to.ToContext;
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
    private ToContext toContext;

    public SyncCommandListener(Context toContext) {
        super(toContext);
        this.toContext = (ToContext) toContext;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        Thread thread = new Thread(() -> {
            while (!toContext.isClose()) {
                if (ctx.channel().pipeline().get(Constant.SLOT_HANDLER_NAME) == null) {
                    String command = toContext.listen();
                    if (command != null) {
                        ctx.writeAndFlush(command);
                        logger.debug("Write command success [{}]", command);
                    }
                }
            }
        });
        thread.setName(Constant.PROJECT_NAME + "-To-Writer-" + toContext.getHost() + ":" + toContext.getPort());
        thread.start();
    }

    @Override
    public void channelRead1(ChannelHandlerContext ctx, String reply) throws Exception {
        logger.debug("Receive redis reply [{}]", reply);
        toContext.callBack(reply);
    }
}
