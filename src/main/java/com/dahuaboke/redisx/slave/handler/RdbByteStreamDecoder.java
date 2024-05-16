package com.dahuaboke.redisx.slave.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.command.slave.RdbCommand;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/6 11:09
 * auth: dahua
 * desc: Rdb文件解析处理类
 */
public class RdbByteStreamDecoder extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(RdbByteStreamDecoder.class);
    private int rdbSize = 0;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof RdbCommand) {
            RdbCommand rdb = (RdbCommand) msg;
            logger.info("Now processing the RDB stream");
            ByteBuf in = rdb.getIn();
            //池化堆内存，因为堆内存创建比较快，这部分数据比较小，所以不用直接内存
            ByteBuf headBuf = ByteBufAllocator.DEFAULT.heapBuffer(20);
            while (in.isReadable()) {
                byte b = in.readByte();
                if (b == '\n') {
                    break;
                }
                if (b != '\r') {
                    headBuf.writeByte(b);
                }
            }
            if (headBuf.isReadable()) {
                String rdbSizeCommand = headBuf.toString(CharsetUtil.UTF_8);
                headBuf.release();
                int streamSize = in.readableBytes();
                if (rdbSizeCommand.startsWith("$")) {
                    rdbSize = Integer.parseInt(rdbSizeCommand.substring(1));
                    if (rdbSize == streamSize) {
                        System.out.println(in.toString(CharsetUtil.UTF_8));
                        logger.info("The RDB stream has been processed");
                        ctx.channel().attr(Constant.RDB_STREAM_NEXT).set(false);
                    }
                    //需要等下一次流
                } else if (rdbSize == streamSize) {
                    System.out.println("分开");
                    System.out.println(in.toString(CharsetUtil.UTF_8));
                    rdbSize = 0;
                    logger.info("The RDB stream has been processed");
                    ctx.channel().attr(Constant.RDB_STREAM_NEXT).set(false);
                } else {
                    logger.error("The RDB stream processed error");
                    ctx.channel().attr(Constant.RDB_STREAM_NEXT).set(false);
                }
            } else {
                //空指令跳过
                headBuf.release();
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }
}
