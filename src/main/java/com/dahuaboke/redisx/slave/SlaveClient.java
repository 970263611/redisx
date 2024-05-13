package com.dahuaboke.redisx.slave;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.slave.handler.*;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/6 11:02
 * auth: dahua
 * desc: redis模拟从节点客户端
 */
public class SlaveClient {

    private static final Logger logger = LoggerFactory.getLogger(RdbByteStreamDecoder.class);

    private SlaveContext slaveContext;
    private EventLoopGroup group = new NioEventLoopGroup(1);

    public SlaveClient(SlaveContext slaveContext) {
        this.slaveContext = slaveContext;
    }

    /**
     * 启动方法
     */
    public void start() {
        String masterHost = slaveContext.getMasterHost();
        int masterPort = slaveContext.getMasterPort();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer() {
                        @Override
                        protected void initChannel(Channel channel) throws Exception {
                            ChannelPipeline pipeline = channel.pipeline();
                            pipeline.addLast(Constant.INIT_SYNC_HANDLER_NAME, new SyncInitializationHandler(slaveContext));
                            pipeline.addLast(new AckOffsetHandler(slaveContext));
                            pipeline.addLast(new PreDistributeHandler());
                            pipeline.addLast(new OffsetCommandDecoder());
                            pipeline.addLast(new RdbByteStreamDecoder());
                            pipeline.addLast(new RedisDecoder());
                            pipeline.addLast(new RedisBulkStringAggregator());
                            pipeline.addLast(new RedisArrayAggregator());
                            pipeline.addLast(new MessagePostProcessor());
                            pipeline.addLast(new PostDistributeHandler());
                            pipeline.addLast(new SyncCommandPublisher(slaveContext));
                            pipeline.addLast(new PingCommandDecoder());
                        }
                    });
            logger.info("Slave will start at {} {}", masterHost, masterPort);
            bootstrap.connect(masterHost, masterPort).sync();
        } catch (InterruptedException e) {
            logger.error("Connect to {} {} exception", masterHost, masterPort, e);
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
