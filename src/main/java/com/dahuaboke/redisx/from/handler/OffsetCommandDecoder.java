package com.dahuaboke.redisx.from.handler;

import com.dahuaboke.redisx.common.Constants;
import com.dahuaboke.redisx.common.command.from.OffsetCommand;
import com.dahuaboke.redisx.from.FromContext;
import io.netty.buffer.ByteBuf;
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
    private FromContext fromContext;

    public OffsetCommandDecoder(FromContext fromContext) {
        this.fromContext = fromContext;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, OffsetCommand msg) throws Exception {
        Channel channel = ctx.channel();
        String masterId = msg.getMasterId();
        if (masterId != null) {
            fromContext.setMasterId(masterId);
        } else {
            logger.error("MasterId is null");
        }
        long offset = msg.getOffset();
        fromContext.setOffset(offset);
        logger.debug("Set masterId [{}] offset [{}]", masterId, offset);
        ctx.pipeline().addAfter(Constants.OFFSET_DECODER_NAME, Constants.OFFSET_HANDLER_NAME, new AckOffsetHandler(fromContext));
        ByteBuf in = msg.getIn();
        if (in != null) {
            ctx.fireChannelRead(in);
        }
    }
}
