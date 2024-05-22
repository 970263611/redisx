package com.dahuaboke.redisx.slave;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.handler.CommandEncoder;
import com.dahuaboke.redisx.handler.DirtyDataHandler;
import com.dahuaboke.redisx.handler.SlotInfoHandler;
import com.dahuaboke.redisx.slave.handler.*;
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
 * 2024/5/6 11:02
 * auth: dahua
 * desc: redis模拟从节点客户端
 */
public class SlaveClient {

    private static final Logger logger = LoggerFactory.getLogger(RdbByteStreamDecoder.class);

    private SlaveContext slaveContext;
    private EventLoopGroup group;
    private Channel channel;

    public SlaveClient(SlaveContext slaveContext, Executor executor) {
        this.slaveContext = slaveContext;
        group = new NioEventLoopGroup(1, executor);
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
                            boolean console = slaveContext.isConsole();
                            pipeline.addLast(new RedisEncoder());
                            pipeline.addLast(new CommandEncoder());
                            if (!console) {
                                pipeline.addLast(Constant.INIT_SYNC_HANDLER_NAME, new SyncInitializationHandler(slaveContext));
                                pipeline.addLast(new AckOffsetHandler(slaveContext));
                                pipeline.addLast(new PreDistributeHandler());
                                pipeline.addLast(new OffsetCommandDecoder());
                                pipeline.addLast(new RdbByteStreamDecoder(slaveContext));
                            }
                            pipeline.addLast(new RedisDecoder(true));
                            pipeline.addLast(new RedisBulkStringAggregator());
                            pipeline.addLast(new RedisArrayAggregator());
                            if (slaveContext.isMasterIsCluster()) {
                                pipeline.addLast(Constant.SLOT_HANDLER_NAME, new SlotInfoHandler(slaveContext));
                            }
                            pipeline.addLast(new MessagePostProcessor());
                            pipeline.addLast(new PostDistributeHandler());
                            pipeline.addLast(new SyncCommandPublisher(slaveContext));
                            pipeline.addLast(new PingCommandDecoder());
                            pipeline.addLast(new DirtyDataHandler());
                        }
                    });
            channel = bootstrap.connect(masterHost, masterPort).sync().channel();
            slaveContext.setSlaveChannel(channel);
            channel.closeFuture().addListener((ChannelFutureListener) future -> {
                slaveContext.setClose(true);
            }).sync();
            logger.info("Slave start at [{}:{}]", masterHost, masterPort);
        } catch (InterruptedException e) {
            logger.error("Connect to [{}:{}] exception", masterHost, masterPort, e);
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
        slaveContext.setClose(true);
        if (channel != null) {
            String masterHost = slaveContext.getMasterHost();
            int masterPort = slaveContext.getMasterPort();
            channel.close();
            logger.info("Close slave [{}:{}]", masterHost, masterPort);
        }
    }
}
