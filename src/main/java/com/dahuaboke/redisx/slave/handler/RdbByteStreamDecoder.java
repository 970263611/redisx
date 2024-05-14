package com.dahuaboke.redisx.slave.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.command.slave.RdbCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/6 11:09
 * auth: dahua
 * desc: Rdb文件解析处理类
 */
public class RdbByteStreamDecoder extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(RdbByteStreamDecoder.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof RdbCommand) {
            RdbCommand rdb = (RdbCommand) msg;
            logger.info("Now processing the RDB stream");
            ByteBuf in = rdb.getIn();
            int readTotal = in.readableBytes();
            if (readTotal > 9) {
                ByteBuf slice = in.slice(readTotal - 9, 9);
                //10-255 16-ff
                if (255 == (slice.readByte() & 0xff)) {
                    //TODO
                    logger.info("The RDB stream has been processed");
                    ReferenceCountUtil.release(msg);
                    ctx.channel().attr(Constant.RDB_STREAM_NEXT).set(false);
                }
            }
            return;
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    public int byte2Int(Byte[] bytes) {
        return (bytes[0] & 0xff) << 24
                | (bytes[1] & 0xff) << 16
                | (bytes[2] & 0xff) << 8
                | (bytes[3] & 0xff);
    }
}
