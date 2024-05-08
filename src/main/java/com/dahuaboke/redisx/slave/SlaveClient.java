package com.dahuaboke.redisx.slave;

import com.dahuaboke.redisx.slave.handler.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/6 11:02
 * auth: dahua
 * desc: redis模拟从节点客户端
 */
public class SlaveClient {

    private static final Logger logger = LoggerFactory.getLogger(RdbCommandDecoder.class);

    private String masterHost;
    private int masterPort;

    public SlaveClient(String masterHost, int masterPort) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
    }

    /**
     * 启动方法
     */
    public void start() {
        EventLoopGroup group = new NioEventLoopGroup(1);
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer() {
                        @Override
                        protected void initChannel(Channel channel) throws Exception {
                            ChannelPipeline pipeline = channel.pipeline();
                            pipeline.addLast(SyncCommandConst.INIT_SYNC_HANDLER_NAME, new SyncInitializationHandler());
                            pipeline.addLast(new AckOffsetHandler());
                            pipeline.addLast(new PreCommandHandler());
                            pipeline.addLast(new OffsetCommandDecoder());
                            pipeline.addLast(new RdbCommandDecoder());
                            pipeline.addLast(new SyncCommandDecoder());
                            pipeline.addLast(new SystemCommandDecoder());
                            pipeline.addLast(new PingCommandDecoder());
                        }
                    });
            bootstrap.connect(masterHost, masterPort).sync().channel();
        } catch (InterruptedException e) {
            logger.error(String.format("connection %s %d exception", masterHost, masterPort), e);
        } finally {
            //TODO 关闭
        }
    }

    /**
     * 销毁方法
     */
    public void destroy() {

    }
}
