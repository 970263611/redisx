package com.dahuaboke.redisx.to.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.command.from.SyncCommand;
import com.dahuaboke.redisx.from.FromContext;
import com.dahuaboke.redisx.to.ToContext;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.redis.RedisMessage;
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
        int flushSize = toContext.getFlushSize();
        Thread thread = new Thread(() -> {
            Channel channel = ctx.channel();
            int flushThreshold = 0;
            long timeThreshold = System.currentTimeMillis();
            while (!toContext.isClose()) {
                if (channel.pipeline().get(Constant.SLOT_HANDLER_NAME) == null && channel.isActive()) {
                    SyncCommand syncCommand = toContext.listen();
                    boolean immediate = toContext.isImmediate();
                    if (syncCommand != null) {
                        FromContext fromContext = (FromContext) syncCommand.getContext();
                        int length = syncCommand.getSyncLength();
                        RedisMessage redisMessage = syncCommand.getRedisMessage();
//                        List<String> command = syncCommand.getCommand();
                        long offset = fromContext.getOffset();
                        if (syncCommand.isNeedAddLengthToOffset()) {
                            offset += length;
                            fromContext.setOffset(offset);
                        }
                        if (immediate) { //强一致模式
                            for (int i = 0; i < toContext.getImmediateResendTimes(); i++) {
                                boolean success = immediateSend(ctx, redisMessage, length, i + 1);
                                if (success) {
                                    break;
                                }
                            }
                        } else {
                            ctx.write(redisMessage);
                            flushThreshold++;
                        }
                        logger.debug("Write length [{}], now offset [{}]", length, offset);
                    }
                    if (!immediate && (flushThreshold > flushSize || (System.currentTimeMillis() - timeThreshold > 100))) {
                        if (flushThreshold > 0) {
                            ctx.flush();
                            logger.trace("Flush data success [{}]", flushThreshold);
                            flushThreshold = 0;
                            timeThreshold = System.currentTimeMillis();
                        }
                    }
                }
            }
        });
        thread.setName(Constant.PROJECT_NAME + "-To-Writer-" + toContext.getHost() + ":" + toContext.getPort());
        thread.start();
    }

    private boolean immediateSend(ChannelHandlerContext ctx, RedisMessage redisMessage, int length, int times) {
        if (!ctx.writeAndFlush(redisMessage).isSuccess() || toContext.preemptMasterCompulsoryWithCheckId()) {
            logger.error("Write length [{}] error times: [" + times + "]", length);
            return false;
        }
        return true;
    }
}