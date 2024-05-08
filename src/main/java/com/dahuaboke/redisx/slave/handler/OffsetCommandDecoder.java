package com.dahuaboke.redisx.slave.handler;

import com.dahuaboke.redisx.slave.SyncCommandConst;
import com.dahuaboke.redisx.slave.command.OffsetCommand;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/8 13:25
 * auth: dahua
 * desc:
 */
public class OffsetCommandDecoder extends SimpleChannelInboundHandler<OffsetCommand> {

    private static final Logger logger = LoggerFactory.getLogger(OffsetCommandDecoder.class);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, OffsetCommand msg) throws Exception {
//        System.out.println(msg.getMasterId());
//        System.out.println(msg.getOffset());
    }
}
