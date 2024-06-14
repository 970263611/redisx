package com.dahuaboke.redisx.from;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.from.handler.*;
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
     */
    public void start() {
        String masterHost = fromContext.getHost();
        int masterPort = fromContext.getPort();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer() {
                        @Override
                        protected void initChannel(Channel channel) throws Exception {
                            ChannelPipeline pipeline = channel.pipeline();
                            boolean console = fromContext.isConsole();
                            pipeline.addLast(new RedisEncoder());
                            pipeline.addLast(new CommandEncoder());
                            if (!console) {
                                pipeline.addLast(Constant.INIT_SYNC_HANDLER_NAME, new SyncInitializationHandler(fromContext));
                                pipeline.addLast(new PreDistributeHandler());
                                pipeline.addLast(new AckOffsetHandler(fromContext));
                                pipeline.addLast(new OffsetCommandDecoder());
                                pipeline.addLast(new RdbByteStreamDecoder(fromContext));
                            }
                            pipeline.addLast(new RedisDecoder(true));
                            pipeline.addLast(new RedisBulkStringAggregator());
                            pipeline.addLast(new RedisArrayAggregator());
                            if (fromContext.isFromIsCluster()) {
                                pipeline.addLast(Constant.SLOT_HANDLER_NAME, new SlotInfoHandler(fromContext));
                            }
                            pipeline.addLast(new MessagePostProcessor());
                            pipeline.addLast(new PostDistributeHandler());
                            pipeline.addLast(new SyncCommandPublisher(fromContext));
                            pipeline.addLast(new PingCommandDecoder());
                            pipeline.addLast(new DirtyDataHandler());
                        }
                    });
            channel = bootstrap.connect(masterHost, masterPort).sync().channel();
            fromContext.setFromChannel(channel);
            logger.info("From start at [{}:{}]", masterHost, masterPort);
            channel.closeFuture().addListener((ChannelFutureListener) future -> {
                fromContext.setClose(true);
            }).sync();
        } catch (InterruptedException e) {
            logger.error("Connect to [{}:{}] exception", masterHost, masterPort, e);
        } finally {
            fromContext.close();
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
        fromContext.setClose(true);
        if (channel != null) {
            String masterHost = fromContext.getHost();
            int masterPort = fromContext.getPort();
            channel.close();
            logger.info("Close from [{}:{}]", masterHost, masterPort);
        }
    }
}
