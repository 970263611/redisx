package com.dahuaboke.redisx.slave.handler;

import com.dahuaboke.redisx.slave.command.RdbSyncCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/6 11:09
 * auth: dahua
 * desc: Rdb文件解析处理类
 */
public class RdbCommandDecoder extends SimpleChannelInboundHandler<RdbSyncCommand> {

    private static final Logger logger = LoggerFactory.getLogger(RdbCommandDecoder.class);

    @Override
    public void channelRead0(ChannelHandlerContext ctx, RdbSyncCommand msg) throws Exception {
        ByteBuf in = msg.getIn();
        System.out.println(in.toString(CharsetUtil.UTF_8));
    }
}
