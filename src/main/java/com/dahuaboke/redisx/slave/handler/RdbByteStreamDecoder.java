package com.dahuaboke.redisx.slave.handler;

import com.dahuaboke.redisx.command.slave.RdbCommand;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/6 11:09
 * auth: dahua
 * desc: Rdb文件解析处理类
 */
public class RdbByteStreamDecoder extends SimpleChannelInboundHandler<RdbCommand> {

    private static final Logger logger = LoggerFactory.getLogger(RdbByteStreamDecoder.class);

    @Override
    public void channelRead0(ChannelHandlerContext ctx, RdbCommand msg) throws Exception {
        logger.info("Now processing the RDB stream");
//        ByteBuf in = msg.getIn();
//        System.out.println(in.toString(CharsetUtil.UTF_8));
        logger.info("The RDB stream has been processed");
    }
}
