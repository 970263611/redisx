package com.dahuaboke.redisx.slave.handler;

import com.dahuaboke.redisx.slave.command.IntegerCommand;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/6 11:09
 * auth: dahua
 * desc: ping解析处理类
 */
public class IntegerCommandDecoder extends SimpleChannelInboundHandler<IntegerCommand> {

    private static final Logger logger = LoggerFactory.getLogger(IntegerCommandDecoder.class);

    @Override
    public void channelRead0(ChannelHandlerContext ctx, IntegerCommand msg) throws Exception {
        System.out.println("receive int message " + msg.getCommand());
    }
}
