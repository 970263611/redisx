package com.dahuaboke.redisx.from.handler;

import com.dahuaboke.redisx.common.Constants;
import com.dahuaboke.redisx.common.cache.CacheManager;
import com.dahuaboke.redisx.from.FromContext;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.dahuaboke.redisx.from.handler.SyncInitializationHandler.State.*;

/**
 * 2024/5/8 11:13
 * auth: dahua
 * desc: 发起同步处理器
 */
public class SyncInitializationHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(SyncInitializationHandler.class);

    private FromContext fromContext;

    public SyncInitializationHandler(FromContext fromContext) {
        this.fromContext = fromContext;
    }

    enum State {
        INIT,
        SENT_PING,
        SENT_PORT,
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
                boolean redisVersionBeyond3 = fromContext.redisVersionBeyond3();
                while (!fromContext.isClose()) {
                    if (channel.pipeline().get(Constants.AUTH_HANDLER_NAME) == null) {
                        if ((reply = channel.attr(Constants.SYNC_REPLY).get()) != null) {
                            if (state == INIT) {
                                state = SENT_PING;
                            }
                            if (Constants.PONG_COMMAND.equalsIgnoreCase(reply) && state == SENT_PING) {
                                clearReply(ctx);
                                if (redisVersionBeyond3) {
                                    state = SENT_PORT;
                                } else {
                                    state = SENT_CAPA;
                                }
                                channel.writeAndFlush(Constants.CONFIG_PORT_COMMAND_PREFIX);
                                logger.info("Sent replconf listening-port command [{}]", Constants.CONFIG_PORT_COMMAND_PREFIX);
                            }
                            if (Constants.OK_COMMAND.equalsIgnoreCase(reply) && state == SENT_PORT) {
                                clearReply(ctx);
                                state = SENT_CAPA;
                                channel.writeAndFlush(Constants.CONFIG_CAPA_COMMAND);
                                logger.info("Sent replconf capa eof command");
                                continue;
                            }
                            if (Constants.OK_COMMAND.equalsIgnoreCase(reply) && state == SENT_CAPA) {
                                clearReply(ctx);
                                state = SENT_PSYNC;
                                CacheManager.NodeMessage nodeMessage = fromContext.getNodeMessage();
                                String command = Constants.CONFIG_PSYNC_COMMAND;
                                if (redisVersionBeyond3) {
                                    if (fromContext.isAlwaysFullSync()) {
                                        command += "? -1";
                                    } else {
                                        if (nodeMessage == null || nodeMessage.getMasterId() == null ||
                                                "null".equalsIgnoreCase(nodeMessage.getMasterId())) {
                                            command += "? -1";
                                        } else {
                                            //从offset的下一位开始获取（包含）
                                            command += nodeMessage.getMasterId() + " " + (nodeMessage.getOffset() + 1);
                                        }
                                    }
                                } else {
                                    command = Constants.CONFIG_SYNC_COMMAND;
                                }
                                channel.writeAndFlush(command);
                                logger.info("Sentpsync " + command + " command");
                            }
                            if (state == SENT_PSYNC) {
                                ChannelPipeline pipeline = channel.pipeline();
                                pipeline.remove(this);
                                logger.info("Sent all sync command");
                                break;
                            }
                        } else {
                            if (state == null) {
                                state = INIT;
                                channel.writeAndFlush(Constants.PING_COMMAND);
                                logger.info("Sent ping command");
                            }
                        }
                    }
                }
            });
            thread.setName(Constants.PROJECT_NAME + "-SYNC-INIT-" + fromContext.getHost() + ":" + fromContext.getPort());
            thread.start();
            ctx.fireChannelActive();
        }
    }

    private void clearReply(ChannelHandlerContext ctx) {
        ctx.channel().attr(Constants.SYNC_REPLY).set(null);
    }
}
