package com.dahuaboke.redisx.from;

import com.dahuaboke.redisx.common.Constants;
import com.dahuaboke.redisx.common.enums.Mode;
import com.dahuaboke.redisx.from.handler.*;
import com.dahuaboke.redisx.handler.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.redis.RedisArrayAggregator;
import io.netty.handler.codec.redis.RedisBulkStringAggregator;
import io.netty.handler.codec.redis.RedisDecoder;
import io.netty.handler.codec.redis.RedisEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

/**
 * 2024/5/6 11:02
 * auth: dahua
 * desc: redis模拟从节点客户端
 */
public class FromClient {

    private static final Logger logger = LoggerFactory.getLogger(RdbByteStreamDecoder.class);

    private FromContext fromContext;
    private EventLoopGroup group;
    private Channel channel;

    public FromClient(FromContext fromContext, Executor executor) {
        this.fromContext = fromContext;
        group = new NioEventLoopGroup(1, executor);
    }

    /**
     * 启动方法
     *
     * @param flag
     */
    public void start(CountDownLatch flag) {
        String masterHost = fromContext.getHost();
        int masterPort = fromContext.getPort();
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer() {
                    @Override
                    protected void initChannel(Channel channel) throws Exception {
                        ChannelPipeline pipeline = channel.pipeline();
                        boolean hasPassword = false;
                        String password = fromContext.getPassword();
                        if (password != null && !password.isEmpty()) {
                            hasPassword = true;
                        }
                        pipeline.addLast(new RedisEncoder());
                        pipeline.addLast(new CommandEncoder());
                        pipeline.addLast(new PrintHandler());
                        if (hasPassword) {
                            pipeline.addLast(Constants.AUTH_HANDLER_NAME, new AuthHandler(password));
                        }
                        if (!fromContext.startByConsole() && !fromContext.isNodesInfoContext()) {
                            pipeline.addLast(Constants.INIT_SYNC_HANDLER_NAME, new SyncInitializationHandler(fromContext));
                            pipeline.addLast(new PreDistributeHandler(fromContext));
                            pipeline.addLast(Constants.OFFSET_DECODER_NAME, new OffsetCommandDecoder(fromContext));
                            pipeline.addLast(new RdbByteStreamDecoder(fromContext));
                        }
                        pipeline.addLast(new RedisDecoder(true));
                        pipeline.addLast(new RedisBulkStringAggregator());
                        pipeline.addLast(new RedisArrayAggregator());
                        if (fromContext.isNodesInfoContext()) {
                            if (Mode.CLUSTER == fromContext.getFromMode()) {
                                pipeline.addLast(Constants.CLUSTER_HANDLER_NAME, new ClusterInfoHandler(fromContext, hasPassword));
                            }
                            if (Mode.SENTINEL == fromContext.getFromMode()) {
                                pipeline.addLast(Constants.SENTINEL_HANDLER_NAME, new SentinelInfoHandler(fromContext, fromContext.getFromMasterName(), fromContext.isGetMasterNodeInfo()));
                            }
                        } else {
                            pipeline.addLast(new MessagePostProcessor(fromContext));
                            pipeline.addLast(new PostDistributeHandler());
                            pipeline.addLast(new SyncCommandPublisher(fromContext));
                            pipeline.addLast(new DirtyDataHandler());
                        }
                    }
                });
        ChannelFuture sync = bootstrap.connect(masterHost, masterPort).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                logger.info("[From] started at [{}:{}]", masterHost, masterPort);
                flag.countDown();
            }
            if (future.cause() != null) {
                logger.info("[From] start error", future.cause());
            }
        });
        channel = sync.channel();
        fromContext.setFromChannel(channel);
    }

    public void sendCommand(String command) {
        if (channel.isActive()) {
            channel.writeAndFlush(command);
        }
    }

    /**
     * 销毁方法
     */
    public void destroy() {
        fromContext.setClose(true);
        if (channel != null && channel.isActive()) {
            channel.close();
            try {
                channel.closeFuture().addListener((ChannelFutureListener) channelFuture -> {
                    if (channelFuture.isSuccess()) {
                        group.shutdownGracefully();
                        logger.info("Close [From] [{}:{}]", fromContext.getHost(), fromContext.getPort());
                    } else {
                        logger.error("Close [From] error", channelFuture.cause());
                    }
                }).sync();
            } catch (InterruptedException e) {
                logger.error("Close [From] error", e);
            }
        }
    }
}
