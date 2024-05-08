package com.dahuaboke.redisx.slave.handler;

import com.dahuaboke.redisx.slave.SyncCommandConst;
import com.dahuaboke.redisx.slave.command.OffsetCommand;
import com.dahuaboke.redisx.slave.command.RdbSyncCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/8 12:52
 * auth: dahua
 * desc:
 */
public class PreCommandHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(PreCommandHandler.class);

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        ctx.channel().attr(SyncCommandConst.RDB_STREAM_NEXT).set(false);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            ByteBuf in = (ByteBuf) msg;
            if (ctx.channel().attr(SyncCommandConst.RDB_STREAM_NEXT).get()) {
                if (in.isReadable()) {
                    if (in.getByte(0) == '$') {
                        ctx.channel().attr(SyncCommandConst.RDB_STREAM_NEXT).set(false);
                        ctx.fireChannelRead(new RdbSyncCommand(in));
                    }
                } else {
                    return;
                }
            } else {
                ByteBuf slice = in.slice(0, 11);
                if (SyncCommandConst.FULLRESYNC.equalsIgnoreCase(slice.toString(CharsetUtil.UTF_8))) {
                    ByteBuf masterAndOffset = in.slice(0, in.readableBytes() - 2);
                    ctx.channel().attr(SyncCommandConst.RDB_STREAM_NEXT).set(true);
                    ctx.fireChannelRead(new OffsetCommand(masterAndOffset.toString(CharsetUtil.UTF_8)));
                } else {
                    ctx.fireChannelRead(in);
                }
            }
        }
    }
}
