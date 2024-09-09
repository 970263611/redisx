package com.dahuaboke.redisx.to;

import com.dahuaboke.redisx.common.Constants;
import com.dahuaboke.redisx.common.enums.Mode;
import com.dahuaboke.redisx.handler.*;
import com.dahuaboke.redisx.to.handler.DRHandler;
import com.dahuaboke.redisx.to.handler.SyncCommandListener;
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
import java.util.concurrent.TimeUnit;

/**
 * 2024/5/13 10:32
 * auth: dahua
 * desc:
 */
public class ToClient {

    private static final Logger logger = LoggerFactory.getLogger(ToClient.class);

    private ToContext toContext;
    private EventLoopGroup group;
    private Channel channel;

    public ToClient(ToContext toContext, Executor executor) {
        this.toContext = toContext;
        group = new NioEventLoopGroup(1, executor);
    }

    /**
     * 启动方法
     *
     * @param flag
     */
    public void start(CountDownLatch flag) {
        String host = toContext.getHost();
        int port = toContext.getPort();
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group).channel(NioSocketChannel.class).handler(new ChannelInitializer() {
            @Override
            protected void initChannel(Channel channel) throws Exception {
                ChannelPipeline pipeline = channel.pipeline();
                pipeline.addLast(new RedisEncoder());
                pipeline.addLast(new CommandEncoder());
                boolean hasPassword = false;
                String password = toContext.getPassword();
                if (password != null && !password.isEmpty()) {
                    hasPassword = true;
                }
                if (hasPassword) {
                    pipeline.addLast(Constants.AUTH_HANDLER_NAME, new AuthHandler(password));
                }
                pipeline.addLast(new RedisDecoder(true));
                pipeline.addLast(new RedisBulkStringAggregator());
                pipeline.addLast(new RedisArrayAggregator());
                if (toContext.isNodesInfoContext()) {
                    if (Mode.CLUSTER == toContext.getToMode()) {
                        pipeline.addLast(Constants.CLUSTER_HANDLER_NAME, new ClusterInfoHandler(toContext, hasPassword));
                    }
                    if (Mode.SENTINEL == toContext.getToMode()) {
                        pipeline.addLast(Constants.SENTINEL_HANDLER_NAME, new SentinelInfoHandler(toContext, toContext.getToMasterName(), toContext.isGetMasterNodeInfo()));
                    }
                } else {
                    pipeline.addLast(new DRHandler(toContext));
                    pipeline.addLast(new SyncCommandListener(toContext));
                    pipeline.addLast(new DirtyDataHandler());
                }
            }
        });
        ChannelFuture sync = bootstrap.connect(host, port).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                logger.info("[To] started at [{}:{}]", host, port);
                flag.countDown();
            }
            if (future.cause() != null) {
                logger.info("[To] start error", future.cause());
                group.shutdownGracefully();
            }
        });
        channel = sync.channel();
    }

    public boolean sendCommand(Object command) {
        return sendCommand(command, false);
    }

    public boolean sendCommand(Object command, boolean needIsSuccess) {
        if (channel.isActive()) {
            if (needIsSuccess) {
                CountDownLatch countDownLatch = new CountDownLatch(1);
                ChannelFuture channelFuture = channel.writeAndFlush(command);
                try {
                    channelFuture.addListener((ChannelFutureListener) channelFuture1 -> {
                        if (channelFuture1.isSuccess()) {
                            countDownLatch.countDown();
                        }
                    });
                    return countDownLatch.await(1000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    return false;
                }
            } else {
                channel.writeAndFlush(command);
            }
        }
        return false;
    }

    /**
     * 销毁方法
     */
    public void destroy() {
        toContext.setClose(true);
        if (channel != null) {
            String host = toContext.getHost();
            int port = toContext.getPort();
            Channel flush = channel.flush();
            if (flush.newSucceededFuture().isSuccess()) {
                logger.info("Flush data success [{}] [{}]", host, port);
            } else {
                logger.error("Flush data error [{}] [{}]", host, port);
            }
            try {
                channel.close().addListener((ChannelFutureListener) channelFuture -> {
                    if (channelFuture.isSuccess()) {
                        group.shutdownGracefully();
                        logger.info("Close [To] [{}:{}]", host, port);
                    } else {
                        logger.error("Close [To] error", channelFuture.cause());
                    }
                }).sync();
            } catch (Exception e) {
                group.shutdownGracefully();
                logger.error("Close [To] error", e);
            }
        } else {
            group.shutdownGracefully();
        }
    }
}
