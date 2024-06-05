package com.dahuaboke.redisx.slave.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.command.slave.OffsetCommand;
import com.dahuaboke.redisx.command.slave.RdbCommand;
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

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        ctx.channel().attr(Constant.RDB_STREAM_NEXT).set(false);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            ByteBuf in = (ByteBuf) msg;
            logger.info(ByteBufUtil.prettyHexDump(in).toString());

            if (ctx.pipeline().get(Constant.INIT_SYNC_HANDLER_NAME) != null) {
                ctx.fireChannelRead(in);
            } else if (ctx.channel().attr(Constant.RDB_STREAM_NEXT).get()){
                logger.debug("Receive rdb byteStream length [{}]", in.readableBytes());
                ctx.fireChannelRead(new RdbCommand(in));
            } else{//redis指令流程
                switch (in.getByte(0)){
                    case Constant.PLUS:// + 开头
                        logger.info("redis success message [{}]!",in.toString(CharsetUtil.UTF_8));
                        String headStr = in.readBytes(ByteBufUtil.indexOf(Constant.SEPARAPOR,in)).toString(StandardCharsets.UTF_8);
                        if(Constant.CONTINUE.equals(headStr)){
                            logger.debug("Find continue command do nothing");
                            in.release();
                        } else if (headStr.startsWith(Constant.CONTINUE)) {
                            logger.debug("Find continue command and will reset offset");
                            ctx.fireChannelRead(new OffsetCommand(headStr));
                            in.release();
                        } else if (headStr.startsWith(Constant.FULLRESYNC)){
                            logger.debug("Find fullReSync command");
                            ctx.fireChannelRead(new OffsetCommand(headStr));
                            ctx.channel().attr(Constant.RDB_STREAM_NEXT).set(true);
                            in.release();
                        } else{
                            ctx.fireChannelRead(in);
                        }
                        break;
                    case Constant.MINUS:// - 开头,错误信息
                        in.release();
                        logger.error("redis error message [{}]!",in.toString(CharsetUtil.UTF_8));
                        break;
                    case Constant.STAR:// * 开头，命令信息
                    case Constant.DOLLAR:
                        ctx.fireChannelRead(in);
                        break;
                    default://其他种类开头
                        logger.info("redis other message [{}]!",in.toString(CharsetUtil.UTF_8));
                        in.release();
                }
            }
        }
    }

}
