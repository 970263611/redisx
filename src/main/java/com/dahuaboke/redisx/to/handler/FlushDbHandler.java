package com.dahuaboke.redisx.to.handler;

import com.dahuaboke.redisx.to.ToContext;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * author: dahua
 * date: 2024/9/11 18:21
 */
public class FlushDbHandler extends ChannelInboundHandlerAdapter {

    private ToContext toContext;

    public FlushDbHandler(ToContext toContext) {
        this.toContext = toContext;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        toContext.flushMyself();
        ctx.fireChannelActive();
    }
}
