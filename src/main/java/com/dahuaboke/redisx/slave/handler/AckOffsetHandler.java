package com.dahuaboke.redisx.slave.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.slave.SlaveContext;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/8 9:55
 * auth: dahua
 * desc: 偏移量回复处理器
 */
public class AckOffsetHandler extends ChannelDuplexHandler {

    private static final Logger logger = LoggerFactory.getLogger(AckOffsetHandler.class);
    private SlaveContext slaveContext;
    private long offset;

    public AckOffsetHandler(SlaveContext slaveContext) {
        this.slaveContext = slaveContext;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        logger.debug("Ack offset task beginning");
        Thread heartBeatThread = new Thread(() -> {
            Channel channel = ctx.channel();
            while (true) {
                if (channel.isActive()) {
                    Long offsetSession = channel.attr(Constant.OFFSET).get();
                    if (offsetSession == null) {
                        continue;
                    } else if (offsetSession > -1L) {
                        offset = offsetSession;
                        channel.attr(Constant.OFFSET).set(-1L);
                    }
                    channel.writeAndFlush(Constant.ACK_COMMAND_PREFIX + offset);
                    logger.debug("Ack offset [{}]", offset);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    logger.error("Ack offset thread interrupted");
                }
            }
        });
        heartBeatThread.setName(Constant.PROJECT_NAME + "-AckThread-" + slaveContext.getMasterHost() + ":" + slaveContext.getMasterPort());
        heartBeatThread.setDaemon(true);
        heartBeatThread.start();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            int i = ((ByteBuf) msg).readableBytes();
            logger.debug("Receive command length [{}], before offset [{}]", i, offset);
            offset += i;
        }
        ctx.fireChannelRead(msg);
    }
}
