package com.dahuaboke.redisx.console.handler;

import com.dahuaboke.redisx.common.cache.CacheMonitor;
import com.dahuaboke.redisx.common.command.console.MonitorCommand;
import com.dahuaboke.redisx.common.command.console.ReplyCommand;
import com.dahuaboke.redisx.console.ConsoleContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.Map;

/**
 * author: dahua
 * date: 2024/8/24 13:56
 */
public class MonitorHandler extends SimpleChannelInboundHandler<MonitorCommand> {

    private ObjectMapper objectMapper = new ObjectMapper();
    private ConsoleContext consoleContext;

    public MonitorHandler(ConsoleContext consoleContext) {
        this.consoleContext = consoleContext;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MonitorCommand monitorCommand) throws Exception {
        CacheMonitor cacheMonitor = consoleContext.getCacheMonitor();
        Map result = cacheMonitor.buildMonitor();
        String reply = objectMapper.writeValueAsString(result);
        ctx.fireChannelRead(new ReplyCommand(reply));
    }
}
