package com.dahuaboke.redisx.netty.handler;

import com.dahuaboke.redisx.core.Context;
import com.dahuaboke.redisx.core.Receiver;
import com.dahuaboke.redisx.netty.RedisClient;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;

import java.util.Map;

/**
 * author: dahua
 * date: 2024/2/27 15:48
 */
public class WebReceiveHandler extends SimpleChannelInboundHandler<Map<String, String>> implements Receiver {

    private static final String COMMAND = "command";
    private String remoteHost;
    private int remotePort;
    private Channel channel;
    private Context context;
    private RedisClient redisClient;

    public WebReceiveHandler(String remoteHost, int remotePort) {
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Context context = new Context();
        redisClient = new RedisClient(remoteHost, remotePort);
        redisClient.start(context);
        this.context = context;
        this.channel = ctx.channel();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, Map<String, String> params) throws Exception {
        context.register(this);
        String command = params.get(COMMAND);
        if (command != null) {
            context.send(command);
        }
    }

    @Override
    public void receive(String callBack) {
        FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                HttpResponseStatus.OK,
                Unpooled.copiedBuffer(callBack, CharsetUtil.UTF_8));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
        channel.writeAndFlush(response);
        channel.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        redisClient.destroy();
        this.context.destroy();
    }
}
