package com.dahuaboke.redisx.to;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.handler.AuthHandler;
import com.dahuaboke.redisx.handler.CommandEncoder;
import com.dahuaboke.redisx.handler.DirtyDataHandler;
import com.dahuaboke.redisx.handler.SlotInfoHandler;
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
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer() {
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
                                pipeline.addLast(Constant.AUTH_HANDLER_NAME, new AuthHandler(password, toContext.isToIsCluster()));
                            }
                            pipeline.addLast(new RedisDecoder(true));
                            pipeline.addLast(new RedisBulkStringAggregator());
                            pipeline.addLast(new RedisArrayAggregator());
                            if (toContext.isToIsCluster()) {
                                pipeline.addLast(Constant.SLOT_HANDLER_NAME, new SlotInfoHandler(toContext, hasPassword));
                            }
                            pipeline.addLast(new DRHandler(toContext));
                            pipeline.addLast(new SyncCommandListener(toContext));
                            pipeline.addLast(new DirtyDataHandler());
                        }
                    });
            ChannelFuture sync = bootstrap.connect(host, port).sync().addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    logger.info("Connected redis [{}:{}]", host, port);
                }
                if (future.cause() != null) {
                    logger.info("Connect redis error [{}]", future.cause());
                }
                flag.countDown();
            });
            channel = sync.channel();
            channel.closeFuture().addListener((ChannelFutureListener) future -> toContext.setClose(true)).sync();
        } catch (InterruptedException e) {
            logger.error("Connect to {{}:{}] exception", host, port, e);
        } finally {
            toContext.close();
            group.shutdownGracefully();
        }
    }

    public void sendCommand(Object command) {
        if (channel.isActive()) {
            channel.writeAndFlush(command);
        }
    }

    /**
     * 销毁方法
     */
    public void destroy() {
        toContext.setClose(true);
        if (channel != null) {
            String host = toContext.getHost();
            int port = toContext.getPort();
            channel.close();
            logger.warn("Close to [{}:{}]", host, port);
        }
    }
}
