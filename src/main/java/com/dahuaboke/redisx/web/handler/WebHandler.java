package com.dahuaboke.redisx.web.handler;

import com.dahuaboke.redisx.web.WebContext;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
        
        webContext.publish(webContext, "");
    }
}
