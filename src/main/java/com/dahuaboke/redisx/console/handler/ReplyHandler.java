package com.dahuaboke.redisx.console.handler;

import com.dahuaboke.redisx.common.Constants;
import com.dahuaboke.redisx.common.command.console.ReplyCommand;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

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
                .set("Content-Type", "text/plain")
                .setInt("Content-Length", response.content().readableBytes());
        ctx.writeAndFlush(response);
        ctx.close();
    }
}
