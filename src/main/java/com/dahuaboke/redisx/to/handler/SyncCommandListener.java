package com.dahuaboke.redisx.to.handler;

import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.common.Constants;
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

import java.util.LinkedList;
import java.util.List;

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
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        int flushSize = toContext.getFlushSize();
        Thread thread = new Thread(() -> {
            Channel channel = ctx.channel();
            int flushThreshold = 0;
            long timeThreshold = System.currentTimeMillis();
            List<SyncCommand> commandListTemp = new LinkedList<>();
            while (!toContext.isClose()) {
                try {
                    if (channel.isActive()) {
                        if (toContext.toStarted()) {
                            SyncCommand syncCommand = toContext.listen();
                            boolean immediate = toContext.isImmediate();
                            if (syncCommand != null) {
                                RedisMessage redisMessage = syncCommand.getCommand();
                                if (redisMessage != null) {
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
                                                        if (toContext.isStartConsole()) {
                                                            toContext.addWriteCount();
                                                        }
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
                                        if (!messageWrited) {
                                            if (toContext.isStartConsole()) {
                                                toContext.addErrorCount();
                                            }
                                        }
                                    } else {
                                        commandListTemp.add(syncCommand);
                                        ctx.write(redisMessage);
                                        flushThreshold++;
                                        if (toContext.isStartConsole()) {
                                            toContext.addWriteCount();
                                        }
                                    }
                                } else {
                                    logger.error("RedisMessage is null {}", syncCommand.getStringCommand());
                                }
                            }
                            if (flushThreshold > flushSize || (System.currentTimeMillis() - timeThreshold > 100)) {
                                if (flushThreshold > 0) {
                                    ctx.flush();
                                    updateOffset(commandListTemp);
                                    logger.debug("Flush data success [{}]", flushThreshold);
                                    flushThreshold = 0;
                                }
                                timeThreshold = System.currentTimeMillis();
                            }
                        }
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
        if (syncCommand.isNeedAddLengthToOffset()) {
            fromContext.cacheOffset(syncCommand);
            logger.trace("Write command [{}] length [{}]", syncCommand.getStringCommand(), syncCommand.getSyncLength());
        }
    }

    private void updateOffset(List<SyncCommand> syncCommandList) {
        for (SyncCommand syncCommand : syncCommandList) {
            FromContext fromContext = (FromContext) syncCommand.getContext();
            if (syncCommand.isNeedAddLengthToOffset()) {
                fromContext.cacheOffset(syncCommand);
                logger.trace("Write command [{}] length [{}]", syncCommand.getStringCommand(), syncCommand.getSyncLength());
            }
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        toContext.setToStarted(false);
        super.channelInactive(ctx);
    }
}