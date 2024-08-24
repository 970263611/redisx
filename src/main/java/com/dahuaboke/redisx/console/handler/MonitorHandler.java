package com.dahuaboke.redisx.console.handler;

import com.dahuaboke.redisx.console.ConsoleContext;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;

/**
 * author: dahua
 * date: 2024/8/24 13:56
 */
public class MonitorHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private ConsoleContext consoleContext;

    public MonitorHandler(ConsoleContext consoleContext) {
        this.consoleContext = consoleContext;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, FullHttpRequest fullHttpRequest) throws Exception {

    }
}
