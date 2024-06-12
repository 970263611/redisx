package com.dahuaboke.redisx.from.handler;

import com.dahuaboke.redisx.command.from.PingCommand;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/6 11:09
 * auth: dahua
 * desc: ping解析处理类
 */
public class PingCommandDecoder extends SimpleChannelInboundHandler<PingCommand> {

    private static final Logger logger = LoggerFactory.getLogger(PingCommandDecoder.class);

    @Override
    public void channelRead0(ChannelHandlerContext ctx, PingCommand msg) throws Exception {
        logger.debug("Receive command ping");
    }
}
