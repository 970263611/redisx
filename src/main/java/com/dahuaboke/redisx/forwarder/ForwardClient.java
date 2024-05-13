package com.dahuaboke.redisx.forwarder;

import com.dahuaboke.redisx.forwarder.handler.SyncCommandListener;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.redis.RedisDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/13 10:32
 * auth: dahua
 * desc:
 */
public class ForwardClient {

    private static final Logger logger = LoggerFactory.getLogger(ForwardClient.class);

    private ForwardContext forwardContext;
    private EventLoopGroup group = new NioEventLoopGroup(1);

    public ForwardClient(ForwardContext forwardContext) {
        this.forwardContext = forwardContext;
    }

    /**
     * 启动方法
     */
    public void start() {
        String forwardHost = forwardContext.getForwardHost();
        int forwardPort = forwardContext.getForwardPort();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer() {
                        @Override
                        protected void initChannel(Channel channel) throws Exception {
                            ChannelPipeline pipeline = channel.pipeline();
                            pipeline.addLast(new RedisDecoder());
                            pipeline.addLast(new SyncCommandListener(forwardContext));
                        }
                    });
            logger.info("Connect redis master {} {}", forwardHost, forwardPort);
            bootstrap.connect(forwardHost, forwardPort).sync();
        } catch (InterruptedException e) {
            logger.error("Connect to {} {} exception", forwardHost, forwardPort, e);
            destroy();
        }
    }

    /**
     * 销毁方法
     */
    public void destroy() {
        group.shutdownGracefully();
    }

}
