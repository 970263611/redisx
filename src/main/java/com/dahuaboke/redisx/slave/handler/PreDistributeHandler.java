package com.dahuaboke.redisx.slave.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.command.slave.OffsetCommand;
import com.dahuaboke.redisx.command.slave.RdbCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/8 12:52
 * auth: dahua
 * desc: 预处理分配处理器
 */
public class PreDistributeHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(PreDistributeHandler.class);

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        ctx.channel().attr(Constant.RDB_STREAM_NEXT).set(false);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            ByteBuf in = (ByteBuf) msg;
            if (ctx.channel().attr(Constant.RDB_STREAM_NEXT).get()) {
                if (in.isReadable()) {
                    if (in.getByte(0) == '$') {
                        logger.debug("Receive rdb byteStream length: {}", in.readableBytes());
                        ctx.channel().attr(Constant.RDB_STREAM_NEXT).set(false);
                        ctx.fireChannelRead(new RdbCommand(in));
                    }
                } else {
                    return;
                }
            } else {
                ByteBuf fullResyncC = in.slice(0, 11);
                if (Constant.FULLRESYNC.equalsIgnoreCase(fullResyncC.toString(CharsetUtil.UTF_8))) {
                    logger.debug("Find fullReSync command");
                    ByteBuf masterAndOffset = in.slice(0, in.readableBytes() - 2);
                    ctx.channel().attr(Constant.RDB_STREAM_NEXT).set(true);
                    ctx.fireChannelRead(new OffsetCommand(masterAndOffset.toString(CharsetUtil.UTF_8)));
                } else {
                    ByteBuf continueC = in.slice(0, 9);
                    if (Constant.CONTINUE.equalsIgnoreCase(continueC.toString(CharsetUtil.UTF_8))) {
                        if (in.readableBytes() > 11) {
                            logger.debug("Find continue command and will reset offset");
                            ByteBuf continueAndOffset = in.slice(0, in.readableBytes() - 2);
                            ctx.fireChannelRead(new OffsetCommand(continueAndOffset.toString(CharsetUtil.UTF_8)));
                        } else {
                            logger.debug("Find continue command do nothing");
                            in.release();
                            return;
                        }
                    } else {
                        ctx.fireChannelRead(in);
                    }
                }
            }
        }
    }
}
