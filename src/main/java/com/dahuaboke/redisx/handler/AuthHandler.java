package com.dahuaboke.redisx.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.enums.Mode;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * 2024/6/21 11:07
 * auth: dahua
 * desc:
 */
public class AuthHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(AuthHandler.class);
    private String password;

    public AuthHandler(String password) {
        this.password = password;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        if (channel.isActive()) {
            //不需要去pipeline的底部，所以直接ctx.write
            ctx.writeAndFlush(Constant.CONFIG_AUTH_PREFIX + password);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object obj) throws Exception {
        ByteBuf reply = (ByteBuf) obj;
        if (!Constant.OK_COMMAND.equalsIgnoreCase(reply.slice(1, 2).toString(StandardCharsets.UTF_8))) {
            logger.error("Password error");
            System.exit(0);
        } else {
            ctx.pipeline().remove(this);
            ctx.fireChannelActive();
        }
    }
}
