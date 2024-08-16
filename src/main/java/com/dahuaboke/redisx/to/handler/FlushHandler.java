package com.dahuaboke.redisx.to.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.to.ToContext;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * author: dahua
 * date: 2024/8/16 13:57
 */
public class FlushHandler extends ChannelInboundHandlerAdapter {

    private ToContext toContext;

    public FlushHandler(ToContext toContext) {
        this.toContext = toContext;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ctx.writeAndFlush(Constant.FLUSH_DB_COMMAND);
        toContext.setFlushDbSuccess();
        super.channelActive(ctx);
    }
}
