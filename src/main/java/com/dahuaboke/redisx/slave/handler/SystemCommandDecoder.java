package com.dahuaboke.redisx.slave.handler;

import com.dahuaboke.redisx.slave.SyncCommandConst;
import com.dahuaboke.redisx.slave.command.PingCommand;
import com.dahuaboke.redisx.slave.command.SystemCommand;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/6 11:09
 * auth: dahua
 * desc: 系统指令解析处理类
 */
public class SystemCommandDecoder extends SimpleChannelInboundHandler<SystemCommand> {

    private static final Logger logger = LoggerFactory.getLogger(SystemCommandDecoder.class);

    @Override
    public void channelRead0(ChannelHandlerContext ctx, SystemCommand msg) throws Exception {
        String command = msg.getCommand();
        switch (command) {
            case SyncCommandConst.PING_COMMAND: {
                ctx.fireChannelRead(new PingCommand());
                break;
            }
            default: {
                break;
            }
        }
    }
}
