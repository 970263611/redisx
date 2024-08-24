package com.dahuaboke.redisx.from.handler;

import com.dahuaboke.redisx.common.Constants;
import com.dahuaboke.redisx.common.cache.CacheManager;
import com.dahuaboke.redisx.common.command.from.OffsetCommand;
import com.dahuaboke.redisx.common.command.from.RdbCommand;
import com.dahuaboke.redisx.from.FromContext;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * 2024/5/8 12:52
 * auth: dahua
 * desc: 预处理分配处理器
 */
public class PreDistributeHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(PreDistributeHandler.class);

    private boolean lineBreakFlag = true;

    private FromContext fromContext;

    private boolean firstFlag = true;

    public PreDistributeHandler(FromContext fromContext) {
        this.fromContext = fromContext;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        ctx.channel().attr(Constants.RDB_STREAM_NEXT).set(false);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            ByteBuf in = (ByteBuf) msg;
            if (ctx.pipeline().get(Constants.INIT_SYNC_HANDLER_NAME) != null) {
                ctx.fireChannelRead(in);
            } else if (ctx.channel().attr(Constants.RDB_STREAM_NEXT).get()) {
                logger.debug("Receive rdb byteStream length [{}]", in.readableBytes());
                ctx.fireChannelRead(new RdbCommand(in));
            } else {
                //redis 7.X版本会发空字符串后在fullresync
                if (lineBreakFlag && in.getByte(0) == Constants.LINE_BREAK) {
                    while (in.getByte(in.readerIndex()) == Constants.LINE_BREAK) {
                        in.readByte();
                        if (in.readerIndex() == in.writerIndex()) {
                            in.release();
                            return;
                        }
                    }
                }
                lineBreakFlag = false;
                if (in.getByte(in.readerIndex()) == Constants.PLUS) {
                    String headStr = in.readBytes(ByteBufUtil.indexOf(Constants.SEPARAPOR, in) - in.readerIndex()).toString(StandardCharsets.UTF_8);
                    in.readBytes(Constants.SEPARAPOR.readableBytes());
                    if (headStr.startsWith(Constants.CONTINUE)) {
                        StringBuilder commandStr = new StringBuilder();
                        commandStr.append(Constants.CONTINUE).append(" ");
                        CacheManager.NodeMessage nodeMessage = fromContext.getNodeMessage();
                        commandStr.append(nodeMessage.getMasterId()).append(" ");
                        commandStr.append(nodeMessage.getOffset());
                        logger.info("+COMMAND " + commandStr.toString());
                        ctx.fireChannelRead(new OffsetCommand(commandStr.toString(), in));
                    } else if (headStr.startsWith(Constants.FULLRESYNC)) {
                        logger.info("+COMMAND " + headStr);
                        ctx.fireChannelRead(new OffsetCommand(headStr));
                        ctx.channel().attr(Constants.RDB_STREAM_NEXT).set(true);
                        if (in.isReadable()) {
                            ctx.fireChannelRead(new RdbCommand(in));
                        } else {
                            in.release();
                        }
                    } else {
                        in.readerIndex(0);
                        ctx.fireChannelRead(in);
                    }
                } else if (firstFlag && !fromContext.redisVersionBeyond3()) {
                    firstFlag = false;
                    ctx.channel().attr(Constants.RDB_STREAM_NEXT).set(true);
                    int index = ByteBufUtil.indexOf(Constants.SEPARAPOR, in);
                    long offset = Long.parseLong(in.readBytes(index).toString(Charset.defaultCharset()).substring(1));
                    ctx.fireChannelRead(new OffsetCommand("FULL" + " ? " + offset));
                    in.readerIndex(0);
                    ctx.fireChannelRead(new RdbCommand(in));

                } else {
                    ctx.fireChannelRead(in);
                }
            }
        }
    }

}
