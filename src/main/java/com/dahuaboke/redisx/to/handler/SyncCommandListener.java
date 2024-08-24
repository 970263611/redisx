package com.dahuaboke.redisx.to.handler;

import com.dahuaboke.redisx.common.Constants;
import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.common.command.from.SyncCommand;
import com.dahuaboke.redisx.from.FromContext;
import com.dahuaboke.redisx.to.ToContext;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
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
                try {
                    if (channel.isActive()) {
                        if (toContext.toStarted()) {
                            SyncCommand syncCommand = toContext.listen();
                            boolean immediate = toContext.isImmediate();
                            if (syncCommand != null) {
                                RedisMessage redisMessage = syncCommand.getCommand();
                                if (immediate && syncCommand.isNeedAddLengthToOffset()) { //强一致模式 && 需要记录偏移量（非rdb数据）
                                    boolean messageWrited = false;
                                    boolean offsetWrited = false;
                                    int retryTimes = 0;
                                    int immediateResendTimes = toContext.getImmediateResendTimes();
                                    while (retryTimes < immediateResendTimes && (!messageWrited || !offsetWrited)) {
                                        try {
                                            retryTimes++;
                                            if (!messageWrited) {
                                                ChannelFuture channelFuture = ctx.writeAndFlush(redisMessage);
                                                channelFuture.await();
                                                boolean channelFutureIsSuccess = channelFuture.isSuccess();
                                                if (channelFutureIsSuccess) {
                                                    messageWrited = true;
                                                    updateOffset(syncCommand);
                                                } else {
                                                    continue;
                                                }
                                            }
                                            if (!offsetWrited) {
                                                if (toContext.preemptMasterCompulsoryWithCheckId()) {
                                                    offsetWrited = true;
                                                    logger.trace("[immediate] write success");
                                                }
                                            }
                                        } catch (InterruptedException e) {
                                            logger.error("Write command or offset awaiter error", e);
                                        }
                                    }
                                } else {
                                    updateOffset(syncCommand);
                                    ctx.write(redisMessage);
                                    flushThreshold++;
                                }
                            }
                            if (flushThreshold > flushSize || (System.currentTimeMillis() - timeThreshold > 100)) {
                                if (flushThreshold > 0) {
                                    ctx.flush();
                                    logger.debug("Flush data success [{}]", flushThreshold);
                                    flushThreshold = 0;
                                }
                                timeThreshold = System.currentTimeMillis();
                            }
                        }
                    } else {
                        toContext.setToStarted(false);
                    }
                } catch (Exception e) {
                    logger.error("Sync command thread find error", e);
                }
            }
        });
        thread.setName(Constants.PROJECT_NAME + "-To-Writer-" + toContext.getHost() + ":" + toContext.getPort());
        thread.start();
    }

    private void updateOffset(SyncCommand syncCommand) {
        FromContext fromContext = (FromContext) syncCommand.getContext();
        int length = syncCommand.getSyncLength();
        long offset = fromContext.getOffset();
        if (syncCommand.isNeedAddLengthToOffset()) {
            offset += length;
            fromContext.setOffset(offset);
            logger.trace("Write command [{}] length [{}], now offset [{}]", syncCommand.getStringCommand(), length, offset);
        }
    }
}