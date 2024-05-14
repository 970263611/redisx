package com.dahuaboke.redisx.web.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.cache.CacheManager;
import com.dahuaboke.redisx.web.WebContext;
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
 * 2024/5/14 10:50
 * auth: dahua
 * desc:
 */
public class WebHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final Logger logger = LoggerFactory.getLogger(WebHandler.class);
    private WebContext webContext;

    public WebHandler(WebContext webContext) {
        this.webContext = webContext;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        String commandParam = request.getUri();
        if (commandParam != null && commandParam.startsWith(Constant.WEB_URI_PREFIX)) {
            String[] command = commandParam.split("=");
            if (Constant.WEB_URI_PREFIX.equalsIgnoreCase(command[0]) && command.length > 1) {
                CacheManager.CommandReference commandReference = webContext.publish(URLDecoder.decode(command[1]));
                webContext.listen(commandReference);
                String reply = commandReference.getResult();
                FullHttpResponse response = new DefaultFullHttpResponse(request.protocolVersion(),
                        HttpResponseStatus.OK,
                        Unpooled.wrappedBuffer(reply.getBytes()));
                response.headers()
                        .set("Content-Type", "text/plain")
                        .setInt("Content-Length", response.content().readableBytes());
                ctx.writeAndFlush(response);
            }
        }
        ctx.close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        ctx.close();
    }
}
