package com.dahuaboke.redisx.netty;

import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.netty.handler.RedisMessageHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.redis.RedisArrayAggregator;
import io.netty.handler.codec.redis.RedisBulkStringAggregator;
import io.netty.handler.codec.redis.RedisDecoder;
import io.netty.handler.codec.redis.RedisEncoder;

/**
 * author: dahua
 * date: 2024/2/27 15:42
 */
public class RedisClient {

    private String host;
    private int port;
    private EventLoopGroup group = new NioEventLoopGroup(1);

    public RedisClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void start(Context context) {
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer() {
                        @Override
                        protected void initChannel(Channel channel) throws Exception {
                            ChannelPipeline pipeline = channel.pipeline();
                            pipeline.addLast(new RedisDecoder());
                            pipeline.addLast(new RedisBulkStringAggregator());
                            pipeline.addLast(new RedisArrayAggregator());
                            pipeline.addLast(new RedisEncoder());
                            pipeline.addLast(new RedisMessageHandler(context));
                        }
                    });
            bootstrap.connect(host, port).sync();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void destroy() {
        group.shutdownGracefully();
    }
}
