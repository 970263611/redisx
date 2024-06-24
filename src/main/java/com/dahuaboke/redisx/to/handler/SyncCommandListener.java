package com.dahuaboke.redisx.to.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.cache.CacheManager;
import com.dahuaboke.redisx.from.FromContext;
import com.dahuaboke.redisx.to.ToContext;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/13 10:37
 * auth: dahua
 * desc:
 */
public class SyncCommandListener extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(SyncCommandListener.class);
    private ToContext toContext;

    public SyncCommandListener(Context toContext) {
        this.toContext = (ToContext) toContext;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        Thread thread = new Thread(() -> {
            while (!toContext.isClose()) {
                if (ctx.channel().pipeline().get(Constant.SLOT_HANDLER_NAME) == null) {
                    CacheManager.CommandReference reference = toContext.listen();
                    if (reference != null) {
                        FromContext fromContext = reference.getFromContext();
                        Integer length = reference.getLength();
                        String command = reference.getContent();
                        long offset = fromContext.getOffset();
                        if (length != null) {
                            offset += length;
                            fromContext.setOffset(offset);
                        }
                        long finalOffset = offset;
                        ctx.writeAndFlush(command).addListener((ChannelFutureListener) future -> {
                            boolean success = future.isSuccess();
                            if (!success) {
                                logger.error("Write command error [{}]", future.cause());
                            } else {
                                logger.debug("Write command success [{}] length [{}], now offset [{}]", command, length, finalOffset);
                                if (toContext.isImmediate()) { //强一致模式
                                    toContext.preemptMasterCompulsory();
                                }
                            }
                        });
                    }
                }
            }
        });
        thread.setName(Constant.PROJECT_NAME + "-To-Writer-" + toContext.getHost() + ":" + toContext.getPort());
        thread.start();
    }
}
