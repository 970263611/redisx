package com.dahuaboke.redisx.slave.handler;

import com.dahuaboke.redisx.slave.SyncCommandConst;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 2024/5/8 9:55
 * auth: dahua
 * desc: 偏移量回复处理器
 */
public class AckOffsetHandler extends ChannelDuplexHandler {

    private static final Logger logger = LoggerFactory.getLogger(AckOffsetHandler.class);
    private static final ExecutorService executor = new ThreadPoolExecutor(1, 1, 10L,
            TimeUnit.SECONDS, new LinkedBlockingDeque(), new DefaultThreadFactory(SyncCommandConst.PROJECT_NAME));

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        logger.info("Ack offset task will start");
        executor.execute(() -> {
            Channel channel = ctx.channel();
            while (channel.isActive()) {
                //TODO 心跳
//                Long t = channel.attr(SyncCommandConst.OFFSET).get();
                System.out.println("ackkkkkkkkkkkkkkkkkkkk");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    logger.error("Ack offset thread interrupted");
                }
            }
        });
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        /**
         * 强制关闭
         */
        logger.info("Ack offset task will shutdown");
        executor.shutdownNow();
    }
}
