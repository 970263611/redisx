package com.dahuaboke.redisx.slave.handler;

import com.dahuaboke.redisx.slave.SlaveConst;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.dahuaboke.redisx.slave.handler.SyncInitializationHandler.State.*;

/**
 * 2024/5/8 11:13
 * auth: dahua
 * desc: 发起同步处理器
 */
public class SyncInitializationHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(SyncInitializationHandler.class);

    enum State {
        INIT,
        SENT_PING,
        SENT_PORT,
        SENT_ADDRESS,
        SENT_CAPA,
        SENT_PSYNC;

    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        if (channel.isActive()) {
            Thread thread = new Thread(() -> {
                State state = null;
                String reply;
                for (; ; ) {
                    if ((reply = channel.attr(SlaveConst.SYNC_REPLY).get()) != null) {
                        if (state == INIT) {
                            state = SENT_PING;
                        }
                        if ("PONG".equalsIgnoreCase(reply) && state == SENT_PING) {
                            clearReply(ctx);
                            state = SENT_PORT;
                            channel.writeAndFlush(ByteBufUtil.writeUtf8(ctx.alloc(), "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n8080\r\n"));
                            logger.debug("Sent replconf listening-port command");
                        }
                        if ("OK".equalsIgnoreCase(reply) && state == SENT_PORT) {
                            clearReply(ctx);
                            state = SENT_ADDRESS;
                            channel.writeAndFlush(ByteBufUtil.writeUtf8(ctx.alloc(), "*3\r\n$8\r\nREPLCONF\r\n$10\r\nip-address\r\n$9\r\n127.0.0.1\r\n"));
                            logger.debug("Sent replconf address command");
                            continue;
                        }
                        if ("OK".equalsIgnoreCase(reply) && state == SENT_ADDRESS) {
                            clearReply(ctx);
                            state = SENT_CAPA;
                            channel.writeAndFlush(ByteBufUtil.writeUtf8(ctx.alloc(), "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$3\r\neof\r\n"));
                            logger.debug("Sent replconf capa eof command");
                            continue;
                        }
                        if ("OK".equalsIgnoreCase(reply) && state == SENT_CAPA) {
                            clearReply(ctx);
                            state = SENT_PSYNC;
                            channel.writeAndFlush(ByteBufUtil.writeUtf8(ctx.alloc(), "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"));
                            logger.debug("Sent psync ? -1 command");
                        }
                        if (state == SENT_PSYNC) {
                            ChannelPipeline pipeline = channel.pipeline();
                            pipeline.remove(this);
                            logger.debug("Sent all sync command");
                            break;
                        }
                    } else {
                        if (state == null) {
                            state = INIT;
                            channel.writeAndFlush(ByteBufUtil.writeUtf8(ctx.alloc(), "*1\r\n$4\r\nPING\r\n"));
                            logger.debug("Sent ping command");
                        }
                    }
                }
            });
            thread.setName(SlaveConst.PROJECT_NAME + "-SYNC-INIT");
            thread.setDaemon(true);
            thread.start();
        }
    }

    private void clearReply(ChannelHandlerContext ctx) {
        ctx.channel().attr(SlaveConst.SYNC_REPLY).set(null);
    }
}
