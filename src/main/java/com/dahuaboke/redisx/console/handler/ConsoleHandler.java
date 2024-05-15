package com.dahuaboke.redisx.console.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.console.ConsoleContext;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
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
    private ConsoleContext consoleContext;

    public ConsoleHandler(ConsoleContext consoleContext) {
        this.consoleContext = consoleContext;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        String command = null;
        String type = null;
        String reply = null;
        String param = URLDecoder.decode(request.getUri());
        if (param != null && param.startsWith(Constant.CONSOLE_URI_PREFIX)) {
            String[] commandAry = param.split("\\?");
            if (commandAry.length == 2) {
                String[] commandAndType = commandAry[1].split("&");
                if (commandAndType.length == 2) {
                    String[] commandKAndV = commandAndType[0].split("=");
                    if (commandKAndV.length == 2 && Constant.CONSOLE_COMMAND.equalsIgnoreCase(commandKAndV[0])) {
                        command = commandKAndV[1];
                    }
                    String[] typeKAndV = commandAndType[1].split("=");
                    if (typeKAndV.length == 2 && Constant.CONSOLE_TYPE.equalsIgnoreCase(typeKAndV[0])) {
                        type = typeKAndV[1];
                    }
                }
            }
            if (command != null && type != null) {
                reply = consoleContext.sendCommand(command, type);
            }
            if (reply == null) {
                reply = "Send command error or request param error, uri should be [/console?command=xxx&type=left/right]";
            }
            FullHttpResponse response = new DefaultFullHttpResponse(request.protocolVersion(),
                    HttpResponseStatus.OK,
                    Unpooled.wrappedBuffer(reply.getBytes()));
            response.headers()
                    .set("Content-Type", "text/plain")
                    .setInt("Content-Length", response.content().readableBytes());
            ctx.writeAndFlush(response);
        }
        ctx.close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("Console happen error {}", cause);
        ctx.close();
    }
}
