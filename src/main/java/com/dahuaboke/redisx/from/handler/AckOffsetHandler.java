package com.dahuaboke.redisx.from.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.cache.CacheManager;
import com.dahuaboke.redisx.from.FromContext;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Queue;

/**
 * 2024/5/8 9:55
 * auth: dahua
 * desc: 偏移量回复处理器
 */
public class AckOffsetHandler extends ChannelDuplexHandler {

    private static final Logger logger = LoggerFactory.getLogger(AckOffsetHandler.class);
    private FromContext fromContext;
    private long offset;

    public AckOffsetHandler(FromContext fromContext) {
        this.fromContext = fromContext;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        logger.debug("Ack offset task beginning");
        Thread heartBeatThread = new Thread(() -> {
            Channel channel = ctx.channel();
            while (!fromContext.isClose()) {
                if (channel.isActive() && channel.pipeline().get(Constant.INIT_SYNC_HANDLER_NAME) == null) {
                    Long offsetSession = channel.attr(Constant.OFFSET).get();
                    if (offsetSession == null) {
                        continue;
                    } else if (offsetSession > -1L) {
                        offset = offsetSession;
                        CacheManager.NodeMessage nodeMessage = fromContext.getNodeMessage();
                        if (nodeMessage == null) {
                            fromContext.setOffset(offset);
                        }
                    }
                    Queue<List<String>> offsetQueue = fromContext.getOffsetQueue();
                    while (!offsetQueue.isEmpty()) {
                        computeOffset(offsetQueue.poll());
                    }
                    offset = fromContext.getOffset();
                    channel.writeAndFlush(Constant.ACK_COMMAND_PREFIX + offset);
                    logger.trace("Ack offset [{}]", offset);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    logger.error("Ack offset thread interrupted");
                }
            }
        });
        heartBeatThread.setName(Constant.PROJECT_NAME + "-AckThread-" + fromContext.getHost() + ":" + fromContext.getPort());
        heartBeatThread.start();
    }

    private void computeOffset(List<String> commands) {
        long offset = fromContext.getOffset();
        //*3\r\n
        offset += 1 + String.valueOf(commands.size()).length() + 2;
        for (String command : commands) {
            //$5\r\nabcde\r\n
            int commandLength = command.length();
            int size = 1 + String.valueOf(commandLength).length() + 2 + commandLength + 2;
            offset += size;
        }
        fromContext.setOffset(offset);
    }
}
