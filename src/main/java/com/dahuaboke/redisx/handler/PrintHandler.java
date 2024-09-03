package com.dahuaboke.redisx.handler;

import com.dahuaboke.redisx.from.handler.PreDistributeHandler;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrintHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(PreDistributeHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if(msg != null && msg instanceof ByteBuf  ) {
            ByteBuf in = (ByteBuf) msg;
            StringBuilder sb = new StringBuilder();
            sb.append("\r\n<").append(Thread.currentThread().getName()).append(">")
                    .append("<redis massage> = ").append(in).append("\r\n");
            sb.append(ByteBufUtil.prettyHexDump(in));
            logger.trace(sb.toString());
        }
        ctx.fireChannelRead(msg);
    }
}
