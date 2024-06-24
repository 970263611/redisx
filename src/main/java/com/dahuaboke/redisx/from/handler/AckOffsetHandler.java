package com.dahuaboke.redisx.from.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.from.FromContext;
import com.dahuaboke.redisx.thread.RedisxThreadFactory;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 2024/5/8 9:55
 * auth: dahua
 * desc: 偏移量回复处理器
 */
public class AckOffsetHandler extends ChannelDuplexHandler {

    private static final Logger logger = LoggerFactory.getLogger(AckOffsetHandler.class);
    private ScheduledExecutorService ackPool;
    private FromContext fromContext;

    public AckOffsetHandler(FromContext fromContext) {
        this.fromContext = fromContext;
        this.ackPool = Executors.newScheduledThreadPool(1,
                new RedisxThreadFactory(Constant.PROJECT_NAME + "-AckThread-" + fromContext.getHost() + ":" + fromContext.getPort()));
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        logger.debug("Ack offset task beginning");
        ackPool.scheduleAtFixedRate(() -> {
            try {
                if (fromContext.isClose()) {
                    ackPool.shutdown();
                } else if (!fromContext.isRdbAckOffset()) {
                    fromContext.ackOffset();
                }
            } catch (Exception e) {
                logger.error("Ack offset exception {}", e);
            }
        }, 0, 1, TimeUnit.SECONDS);
    }
}
