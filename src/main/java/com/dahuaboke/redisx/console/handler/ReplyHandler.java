package com.dahuaboke.redisx.console.handler;

import com.dahuaboke.redisx.common.Constants;
import com.dahuaboke.redisx.common.command.console.ReplyCommand;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;

/**
 * author: dahua
 * date: 2024/8/24 14:06
 */
public class ReplyHandler extends SimpleChannelInboundHandler<ReplyCommand> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ReplyCommand replyCommand) throws Exception {
        String reply = replyCommand.getReply();
        HttpVersion httpVersion = ctx.channel().attr(Constants.CONSOLE_HTTP_VERSION).get();
        FullHttpResponse response = new DefaultFullHttpResponse(httpVersion,
                HttpResponseStatus.OK,
                Unpooled.wrappedBuffer(reply.getBytes()));
        response.headers()
                .set("Content-Type", "application/json")
                .setInt("Content-Length", response.content().readableBytes());
        //允许跨域访问
        response.headers().set( HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
        response.headers().set( HttpHeaders.Names.ACCESS_CONTROL_ALLOW_HEADERS, "Origin, X-Requested-With, Content-Type, Accept");
        response.headers().set( HttpHeaders.Names.ACCESS_CONTROL_ALLOW_METHODS, "GET, POST, PUT,DELETE");
        ctx.writeAndFlush(response);
        ctx.close();
    }
}
