package com.dahuaboke.redisx.console.handler;

import com.dahuaboke.redisx.common.Constants;
import com.dahuaboke.redisx.common.command.console.MonitorCommand;
import com.dahuaboke.redisx.common.command.console.ReplyCommand;
import com.dahuaboke.redisx.common.command.console.SearchCommand;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLDecoder;

/**
 * 2024/5/15 11:15
 * auth: dahua
 * desc:
 */
public class ConsoleHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final Logger logger = LoggerFactory.getLogger(ConsoleHandler.class);

    @Override
    public void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        ctx.channel().attr(Constants.CONSOLE_HTTP_VERSION).set(request.protocolVersion());
        String param = URLDecoder.decode(request.getUri());
        if (param != null) {
            if (param.startsWith(Constants.CONSOLE_URI_SEARCH_PREFIX)) {
                String[] params = param.split("\\?");
                ctx.fireChannelRead(new SearchCommand(params));
            } else if (param.startsWith(Constants.CONSOLE_URI_MONITOR_PREFIX)) {
                ctx.fireChannelRead(new MonitorCommand());
            } else {
                ctx.fireChannelRead(new ReplyCommand("Can not adapt uri path"));
            }
        } else {
            ctx.fireChannelRead(new ReplyCommand("Request param error"));
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("Console happen error {}", cause);
        ctx.close();
    }
}
