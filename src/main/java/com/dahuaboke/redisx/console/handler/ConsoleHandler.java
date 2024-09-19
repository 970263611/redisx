package com.dahuaboke.redisx.console.handler;

import com.dahuaboke.redisx.Redisx;
import com.dahuaboke.redisx.common.Constants;
import com.dahuaboke.redisx.common.command.console.MonitorCommand;
import com.dahuaboke.redisx.common.command.console.ReplyCommand;
import com.dahuaboke.redisx.common.command.console.SearchCommand;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
            if (param.equalsIgnoreCase(Constants.CONSOLE_URI_CONSOLE_PREFIX)) {
                InputStream inputStream = Redisx.class.getClassLoader().getResourceAsStream("monitor.html");
                if (inputStream != null) {
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                        StringBuffer sb = new StringBuffer();
                        String line;
                        while ((line = reader.readLine()) != null) {
                            sb.append(line);
                        }
                        ByteBuf content = Unpooled.copiedBuffer(sb, CharsetUtil.UTF_8);
                        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, content);
                        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html; charset=UTF-8");
                        ctx.writeAndFlush(response);
                    } catch (IOException e) {
                    }
                }
                ctx.close();
            }
            if (param.startsWith(Constants.CONSOLE_URI_SEARCH_PREFIX)) {
                String[] params = param.split("\\?");
                ctx.fireChannelRead(new SearchCommand(params));
            } else if (param.startsWith(Constants.CONSOLE_URI_MONITOR_PREFIX)) {
                ctx.fireChannelRead(new MonitorCommand());
            } else if (param.startsWith("images")) {
                System.out.println(param);
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
