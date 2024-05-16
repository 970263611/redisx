package com.dahuaboke.redisx.forwarder;

import com.dahuaboke.redisx.forwarder.handler.SyncCommandListener;
import com.dahuaboke.redisx.handler.CommandEncoder;
import com.dahuaboke.redisx.handler.DirtyDataHandler;
import com.dahuaboke.redisx.handler.SlotInfoHandler;
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
public class ForwarderClient {

    private static final Logger logger = LoggerFactory.getLogger(ForwarderClient.class);

    private ForwarderContext forwarderContext;
    private EventLoopGroup group;
    private Channel channel;

    public ForwarderClient(ForwarderContext forwarderContext, Executor executor) {
        this.forwarderContext = forwarderContext;
        group = new NioEventLoopGroup(1, executor);
    }

    /**
     * 启动方法
     */
    public void start() {
        String forwardHost = forwarderContext.getForwardHost();
        int forwardPort = forwarderContext.getForwardPort();
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
                            if (forwarderContext.isForwarderIsCluster()) {
                                pipeline.addLast(new SlotInfoHandler(forwarderContext));
                            }
                            pipeline.addLast(new SyncCommandListener(forwarderContext));
                            pipeline.addLast(new DirtyDataHandler());
                        }
                    });
            channel = bootstrap.connect(forwardHost, forwardPort).sync().channel();
            channel.closeFuture().addListener((ChannelFutureListener) future -> {
                forwarderContext.setClose(true);
            }).sync();
            logger.info("Connect redis master [{}:{}]", forwardHost, forwardPort);
        } catch (InterruptedException e) {
            logger.error("Connect to {{}:{}] exception", forwardHost, forwardPort, e);
        } finally {
            destroy();
            group.shutdownGracefully();
        }
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
        forwarderContext.setClose(true);
        if (channel != null) {
            String forwardHost = forwarderContext.getForwardHost();
            int forwardPort = forwarderContext.getForwardPort();
            channel.close();
            logger.info("Close forwarder [{}:{}]", forwardHost, forwardPort);
        }
    }
}
