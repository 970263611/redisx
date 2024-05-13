package com.dahuaboke.redisx.slave.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.command.slave.OffsetCommand;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/8 13:25
 * auth: dahua
 * desc: 偏移量和主节点id解码器
 */
public class OffsetCommandDecoder extends SimpleChannelInboundHandler<OffsetCommand> {

    private static final Logger logger = LoggerFactory.getLogger(OffsetCommandDecoder.class);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, OffsetCommand msg) throws Exception {
        Channel channel = ctx.channel();
        String masterId = msg.getMasterId();
        Long offset = msg.getOffset();
        channel.attr(Constant.MASTER_ID).set(masterId);
        channel.attr(Constant.OFFSET).set(offset);
        logger.debug("Set masterId {} offset {}", masterId, offset);
    }
}
