package com.dahuaboke.redisx.from.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.command.from.OffsetCommand;
import com.dahuaboke.redisx.command.from.RdbCommand;
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
            int length = in.readableBytes();
            if (in.readableBytes() == 1 && in.getByte(0) == '\n') {
                //redis 7.X版本会发空字符串后在fullresync
                in.release();
            } else {
                if (ctx.channel().attr(Constant.RDB_STREAM_NEXT).get()) {
                    if (in.isReadable()) {
                        logger.debug("Receive rdb byteStream length [{}]", in.readableBytes());
                        ctx.fireChannelRead(new RdbCommand(in));
                    }
                } else {
                    int indexHead = 0;
                    while ('\n' == in.getByte(indexHead)) {
                        //因为这里可能存在指令前面带着\n的情况，所以先排除\n的长度干扰
                        in.readByte();
                        indexHead++;
                        //重新给length赋值为当前字节流长度
                        length--;
                    }
                    //判断是不是同步指令
                    if (ctx.pipeline().get(Constant.INIT_SYNC_HANDLER_NAME) != null) {
                        ctx.fireChannelRead(in);
                    } else {
                        /**
                         * 3种场景
                         * +FULLRESYNC 40位id offset
                         * +CONTINUE
                         * +CONTINUE offset
                         */
                        if (length == 9) {
                            //continue
                            String isContinue = in.slice(indexHead, 9).toString(CharsetUtil.UTF_8);
                            if (Constant.CONTINUE.equals(isContinue)) {
                                logger.debug("Find continue command do nothing");
                                in.release();
                            }
                        } else if (length > 9) {
                            String isContinue = in.slice(indexHead, 9).toString(CharsetUtil.UTF_8);
                            int index = 0;
                            while (in.isReadable()) {
                                byte b = in.readByte();
                                if ('\n' == b) {
                                    break;
                                }
                                if ('\r' != b) {
                                    index++;
                                }
                            }
                            if (Constant.CONTINUE.equals(isContinue)) {
                                //continue offset
                                String continueAndOffset = in.slice(indexHead, index).toString(CharsetUtil.UTF_8);
                                logger.debug("Find continue command and will reset offset");
                                ctx.fireChannelRead(new OffsetCommand(continueAndOffset));
                                in.release();
                            } else {
                                //fullresync
                                String isFullResync = in.slice(indexHead, 11).toString(CharsetUtil.UTF_8);
                                if (Constant.FULLRESYNC.equals(isFullResync)) {
                                    logger.debug("Find fullReSync command");
                                    String fullResync = in.slice(indexHead, index).toString(CharsetUtil.UTF_8);
                                    ctx.fireChannelRead(new OffsetCommand(fullResync));
                                    ctx.channel().attr(Constant.RDB_STREAM_NEXT).set(true);
                                    in.release();
                                }
                            }
                        }
                        if (in.isReadable()) {
                            ctx.fireChannelRead(in);
                        }
                    }
                }
            }
        }
    }
}
