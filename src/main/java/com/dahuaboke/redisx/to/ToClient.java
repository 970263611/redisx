package com.dahuaboke.redisx.to;

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
     */
    public void start() {
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
                            pipeline.addLast(new RedisDecoder(true));
                            pipeline.addLast(new RedisBulkStringAggregator());
                            pipeline.addLast(new RedisArrayAggregator());
                            if (toContext.isToIsCluster()) {
                                pipeline.addLast(new SlotInfoHandler(toContext));
                            }
                            pipeline.addLast(new DRHandler(toContext));
                            pipeline.addLast(new SyncCommandListener(toContext));
                            pipeline.addLast(new DirtyDataHandler());
                        }
                    });
            channel = bootstrap.connect(host, port).sync().channel();
            logger.info("Connect redis master [{}:{}]", host, port);
            channel.closeFuture().addListener((ChannelFutureListener) future -> {
                toContext.setClose(true);
            }).sync();
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
            logger.info("Close to [{}:{}]", host, port);
        }
    }
}
