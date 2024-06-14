package com.dahuaboke.redisx.from.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.command.from.OffsetCommand;
import com.dahuaboke.redisx.command.from.RdbCommand;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * 2024/5/8 12:52
 * auth: dahua
 * desc: 预处理分配处理器
 */
public class PreDistributeHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(PreDistributeHandler.class);

    private boolean lineBreakFlag = true;

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        ctx.channel().attr(Constant.RDB_STREAM_NEXT).set(false);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            ByteBuf in = (ByteBuf) msg;
            StringBuilder sb = new StringBuilder();
            sb.append("\r\n<").append(Thread.currentThread().getName()).append(">")
                    .append("<redis massage> = ").append(in).append("\r\n");
            sb.append(ByteBufUtil.prettyHexDump(in).toString());
            logger.trace(sb.toString());

            if (ctx.pipeline().get(Constant.INIT_SYNC_HANDLER_NAME) != null) {
                ctx.fireChannelRead(in);
            } else if (ctx.channel().attr(Constant.RDB_STREAM_NEXT).get()){
                logger.debug("Receive rdb byteStream length [{}]", in.readableBytes());
                ctx.fireChannelRead(new RdbCommand(in));
            } else {
                //redis 7.X版本会发空字符串后在fullresync
                if (lineBreakFlag && in.getByte(0) == Constant.LINE_BREAK) {
                    while (in.getByte(in.readerIndex()) == Constant.LINE_BREAK) {
                        in.readByte();
                        if (in.readerIndex() == in.writerIndex()) {
                            in.release();
                            return;
                        }
                    }
                }
                lineBreakFlag = false;
                if (in.getByte(in.readerIndex()) == Constant.PLUS) {
                    String headStr = in.readBytes(ByteBufUtil.indexOf(Constant.SEPARAPOR, in) - in.readerIndex()).toString(StandardCharsets.UTF_8);
                    if (headStr.startsWith(Constant.CONTINUE)) {
                        logger.debug("Find continue command and will reset offset");
                        in.readBytes(Constant.SEPARAPOR.readableBytes());
                        ctx.fireChannelRead(in);
                    } else if (headStr.startsWith(Constant.FULLRESYNC)) {
                        logger.debug("Find fullReSync command");
                        ctx.fireChannelRead(new OffsetCommand(headStr));
                        ctx.channel().attr(Constant.RDB_STREAM_NEXT).set(true);
                        in.release();
                    } else {
                        in.readerIndex(0);
                        ctx.fireChannelRead(in);
                    }
                } else {
                    ctx.fireChannelRead(in);
                }
            }
        }
    }

}
